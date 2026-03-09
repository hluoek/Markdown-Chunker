import yaml
import re
import tiktoken
from typing import List, Tuple, Dict, Optional, Any
from uuid import uuid4
from langchain_core.documents import Document
from langchain_text_splitters import MarkdownHeaderTextSplitter

from schemas import Node, SplitterConfig
from segmenter import BlockSplitter

# --- 智能 Markdown 树形拆分器 ---

class SmartMarkdownTreeSplitter:
    def __init__(self, config_path: str = "markdown_chunker/config.yaml"):
        # 从 YAML 加载拆分参数，并初始化 tokenizer
        self.config = SplitterConfig.from_yaml(config_path)
        self.tokenizer = tiktoken.get_encoding(self.config.chunking_rules.encoding_name)
        self.block_splitter = BlockSplitter(self.config, self.tokenizer)
        # 标题层级映射，按从粗到细的顺序递归下钻
        self.header_levels = [
            ("#", "h1"),
            ("##", "h2"),
            ("###", "h3"),
            ("####", "h4"),
            ("#####", "h5"),
            ("######", "h6"),
        ]

    def _count_tokens(self, text: str) -> int:
        return len(self.tokenizer.encode(text))

    def _extract_front_matter(self, content: str) -> Tuple[dict, str]:
        """提取并移除 Markdown 开头的 Front Matter"""
        pattern = r"^---\s*\n([\s\S]*?)\n---\s*\n"
        match = re.match(pattern, content)
        if match:
            yaml_content = match.group(1)
            try:
                front_matter = yaml.safe_load(yaml_content)
                # 移除 Front Matter 部分
                new_content = content[match.end():]
                return front_matter, new_content
            except yaml.YAMLError:
                pass
        return {}, content

    def _count_elements(self, text: str) -> Tuple[int, int]:
        """统计完整的代码块和表格数量"""
        code_pattern = r"(```(\w+)?\n[\s\S]*?\n```)"
        table_pattern = r"(?m)(^\s*\|.*\|[ \t]*[\r\n]+\s*\|[\s\-\|:]+\|[ \t]*[\r\n]+(?:\s*\|.*\|[ \t]*(?:[\r\n]+|$))*)"
        
        code_blocks = len(re.findall(code_pattern, text))
        tables = len(re.findall(table_pattern, text))
        return code_blocks, tables

    def _build_ast(self, text: str, title: Optional[str]) -> Node:
        """第一阶段：自顶向下构建 AST"""
        return self._recursive_parse(text, 0, title)

    def _recursive_parse(self, text: str, level_idx: int, parent_title: Optional[str], parent_hierarchy: Optional[Dict] = None) -> Node:
        # 每一层递归都携带父层级路径，保证最终 chunk 可追溯来源
        token_count = self._count_tokens(text)
        hierarchy = parent_hierarchy.copy() if parent_hierarchy else {}
        
        # 基准情况 1：足够小，保持原样
        if token_count < self.config.chunking_rules.max_tokens:
            code_count, table_count = self._count_elements(text)
            return Node(
                content=text,
                title=parent_title,
                hierarchy=hierarchy,
                node_type="section",
                complete_codes_count=code_count,
                complete_tables_count=table_count
            )

        # 基准情况 2：没有更多标题可拆分
        if level_idx >= len(self.header_levels):
            return self._split_by_elements(text, parent_title, hierarchy)

        # 递归步骤：尝试按当前标题级别拆分
        current_sep = self.header_levels[level_idx]
        splitter = MarkdownHeaderTextSplitter(headers_to_split_on=[current_sep], strip_headers=False)
        docs = splitter.split_text(text)

        # 辅助函数：更新 hierarchy
        def update_hierarchy_from_doc(doc_metadata: Dict) -> Dict:
            new_h = hierarchy.copy()
            for k, v in doc_metadata.items():
                if k in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                    new_h[k] = v
            return new_h

        # 如果拆分没有降低复杂度（仅返回 1 个文档），立即尝试下一级
        if len(docs) == 1:
            return self._recursive_parse(
                docs[0].page_content, 
                level_idx + 1, 
                parent_title, 
                update_hierarchy_from_doc(docs[0].metadata)
            )

        # 如果成功拆分为多个部分
        section_node = Node(content="", title=parent_title, hierarchy=hierarchy, node_type="section")
        for doc in docs:
            child = self._recursive_parse(
                doc.page_content, 
                level_idx + 1, 
                parent_title, 
                update_hierarchy_from_doc(doc.metadata)
            )
            section_node.add_child(child)
        
        return section_node

    def _split_by_elements(self, text: str, title: Optional[str], hierarchy: Dict) -> Node:
        # 使用 (?m) 多行模式，要求 ``` 必须在行首，防止将结束 ``` 误认为下一个代码块的开头
        code_pattern = r'(?m)(^```[^\n]*\n[\s\S]*?\n^```[^\n]*$)'
        table_pattern = r'((?:[^\n]*\|[^\n]*\n)+[^\n]*\|[^\n]*)'
        pattern = f'{code_pattern}|{table_pattern}'
        parts = re.split(pattern, text)
        children: List[Node] = []
        for part in parts:
            if not part or part.strip() == '':
                continue
            part_stripped = part.strip()
            if part_stripped.startswith('```') and part_stripped.endswith('```'):
                needs_split = self._count_tokens(part_stripped) > self.config.chunking_rules.max_tokens
                children.append(
                    Node(
                        content=part_stripped,
                        title=title,
                        hierarchy=hierarchy.copy(),
                        node_type="code",
                        needs_split=needs_split,
                        complete_codes_count=1,
                        complete_tables_count=0
                    )
                )
                continue
            normalized = re.sub(r"\s", "", part_stripped)
            if '|' in part_stripped and '\n' in part_stripped and re.search(r"\|[-:]+\|", normalized):
                needs_split = self._count_tokens(part_stripped) > self.config.chunking_rules.max_tokens
                children.append(
                    Node(
                        content=part_stripped,
                        title=title,
                        hierarchy=hierarchy.copy(),
                        node_type="table",
                        needs_split=needs_split,
                        complete_codes_count=0,
                        complete_tables_count=1
                    )
                )
                continue
            paragraphs = re.split(r'\n\s*\n', part_stripped)
            for p in paragraphs:
                paragraph = p.strip()
                if paragraph:
                    needs_split = self._count_tokens(paragraph) > self.config.chunking_rules.max_tokens
                    children.append(
                        Node(
                            content=paragraph,
                            title=title,
                            hierarchy=hierarchy.copy(),
                            node_type="text",
                            needs_split=needs_split,
                            complete_codes_count=0,
                            complete_tables_count=0
                        )
                    )
        container = Node(content="", title=title, hierarchy=hierarchy, node_type="section")
        container.children = children
        return container
        
    def _sort_hierarchy(self, hierarchy: Dict[str, Any]) -> Dict[str, Any]:
        """对 hierarchy 字典按 h0, h1... 顺序排序"""
        sorted_hierarchy = {}
        # h0 总是第一位
        if "h0" in hierarchy:
            sorted_hierarchy["h0"] = hierarchy["h0"]
        
        # h1-h99 按数字排序
        header_keys = sorted([k for k in hierarchy.keys() if k.startswith('h') and k[1:].isdigit() and k != 'h0'], key=lambda x: int(x[1:]))
        for k in header_keys:
            sorted_hierarchy[k] = hierarchy[k]
            
        # 其他非 h 开头的 key (如果有)
        other_keys = sorted([k for k in hierarchy.keys() if not (k.startswith('h') and k[1:].isdigit())])
        for k in other_keys:
            sorted_hierarchy[k] = hierarchy[k]
            
        return sorted_hierarchy

    def _flatten_tree(self, node: Node) -> List[Node]:
        """第二阶段：路径投射与展平"""
        if not node.children:
            if node.title:
                # 将文档标题固定写入 h0，作为路径根节点
                node.hierarchy["h0"] = node.title
            
            node.hierarchy = self._sort_hierarchy(node.hierarchy)
            return [node]
        
        flat_list = []
        for child in node.children:
            flat_list.extend(self._flatten_tree(child))
        return flat_list

    def _greedy_packing(self, nodes: List[Node]) -> List[Document]:
        """第三阶段：贪心合并"""
        chunks: List[Document] = []
        current_chunk_nodes: List[Node] = []
        current_tokens = 0

        # 辅助函数：将当前积累的节点作为文档输出，并重置状态
        def flush_current_chunk():
            nonlocal current_chunk_nodes, current_tokens
            if current_chunk_nodes:
                chunks.append(self._merge_nodes_to_doc(current_chunk_nodes))
            current_chunk_nodes = []
            current_tokens = 0

        for node in nodes:
            node_tokens = self._count_tokens(node.content)
            
            # 情况 1: 节点本身需要强制拆分（如超大代码块或表格）
            if node.needs_split:
                flush_current_chunk()
                # 超长单节点不参与常规合并，走专用拆分逻辑
                chunks.extend(self._divide_oversized_node_to_docs(node))
                continue

            # 情况 2: 当前缓冲区为空，直接放入第一个节点
            if not current_chunk_nodes:
                current_chunk_nodes.append(node)
                current_tokens = node_tokens
                continue

            # 情况 3: 加上新节点后超过最大 Token 限制 -> 先 Flush 旧块，新节点作为新块起点
            if current_tokens + node_tokens > self.config.chunking_rules.max_tokens:
                flush_current_chunk()
                current_chunk_nodes.append(node)
                current_tokens = node_tokens
                continue

            # 情况 4: 可以合并 -> 执行合并逻辑
            current_chunk_nodes.append(node)
            current_tokens += node_tokens

        # 循环结束，处理剩余未输出的块
        flush_current_chunk()
        return chunks

    def _merge_node_hierarchies(self, nodes: List[Node]) -> List[str]:
        merged_hierarchy: List[Dict[str, Any]] = []
        for node in nodes:
            sorted_hierarchy = self._sort_hierarchy(node.hierarchy)
            merged_hierarchy.append(sorted_hierarchy)
        
        path_parts: List[str] = []
        vals: List[str] = []
        for hierarchy in merged_hierarchy:
            vals.clear()
            for value in hierarchy.values():
                vals.append(str(value).strip())
            part = "/".join(vals)
            path_parts.append(part)
        return path_parts

    def _longest_common_prefix_segments(self, split_paths: List[List[str]]) -> List[str]:
        if not split_paths:
            return []
        min_len = min(len(parts) for parts in split_paths)
        prefix: List[str] = []
        for idx in range(min_len):
            candidate = split_paths[0][idx]
            if all(parts[idx] == candidate for parts in split_paths):
                prefix.append(candidate)
            else:
                break
        return prefix

    def _hierarchy_to_text(self, hierarchy_paths: List[str]) -> str:

        unique_paths: List[str] = []
        for path in hierarchy_paths:
            p = str(path).strip()
            if p and p not in unique_paths:
                unique_paths.append(p)
        if not unique_paths:
            return ""

        split_paths: List[List[str]] = []
        for path in unique_paths:
            parts = [seg.strip() for seg in path.split("/") if seg.strip()]
            if parts:
                split_paths.append(parts)
        if not split_paths:
            return ""

        roots: List[str] = []
        for parts in split_paths:
            if parts[0] not in roots:
                roots.append(parts[0])
        root_text = roots[0] if len(roots) == 1 else "、".join(roots)

        lcp = self._longest_common_prefix_segments(split_paths)
        lines: List[str] = [f"[文档主题] {root_text}"]

        if len(lcp) > 1:
            lines.append(f"[所属章节] {'/'.join(lcp)}")

        suffixes: List[str] = []
        for parts in split_paths:
            suffix = "/".join(parts[len(lcp):]) if len(parts) > len(lcp) else ""
            if suffix and suffix not in suffixes:
                suffixes.append(suffix)

        if suffixes:
            lines.append(f"[包含模块] {', '.join(suffixes)}")

        return "\n".join(lines)

    def _enrich_doc_with_hierarchy_text(self, doc: Document) -> None:
        '''为文档添加章节层级文本'''
        if not self.config.element_processing.enrich_enabled:
            return

        hierarchy = doc.metadata.get("hierarchy")
        if not isinstance(hierarchy, list) or not hierarchy:
            return
        hierarchy_text = self._hierarchy_to_text(hierarchy)
        if hierarchy_text:
            doc.page_content = f"{hierarchy_text}\n\n{doc.page_content}"

    def _update_metadata(
        self,
        content: str,
        base_metadata: Optional[Dict[str, Any]] = None,
        has_incomplete: bool = False,
        incomplete_type: Optional[str] = None,
        complete_codes_count: Optional[int] = None,
        complete_tables_count: Optional[int] = None
    ) -> Dict[str, Any]:
        metadata = base_metadata.copy() if base_metadata else {}
        
        metadata.update({
            "token_count": self._count_tokens(content),
            "complete_codes_count": complete_codes_count,
            "complete_tables_count": complete_tables_count,
            "has_incomplete_structure": has_incomplete,
        })
        
        if has_incomplete:
            metadata["incomplete_structure_type"] = incomplete_type
            
        return metadata

    def _merge_nodes_to_doc(
        self,
        nodes: List[Node]
    ) -> Document:
        merged_content = "\n\n".join([node.content for node in nodes])
        hierarchy_paths = self._merge_node_hierarchies(nodes)
        merged_title = next((node.title for node in reversed(nodes) if node.title), None)
        merged_code_blocks = sum(node.complete_codes_count for node in nodes)
        merged_tables = sum(node.complete_tables_count for node in nodes)

        metadata = {}
        if merged_title:
            metadata["title"] = merged_title
        if hierarchy_paths:
            dedup_hierarchy_paths: List[str] = []
            for path in hierarchy_paths:
                if path not in dedup_hierarchy_paths:
                    dedup_hierarchy_paths.append(path)
            if dedup_hierarchy_paths:
                metadata["hierarchy"] = dedup_hierarchy_paths
            
        final_metadata = self._update_metadata(
            content=merged_content,
            base_metadata=metadata,
            has_incomplete=False,
            incomplete_type=None,
            complete_codes_count=merged_code_blocks,
            complete_tables_count=merged_tables
        )
        return Document(
            page_content=merged_content,
            metadata=final_metadata
        )

    def _divide_oversized_node_to_docs(self, node: Node) -> List[Document]:
        """将超过max_tokens的text/code/table进一步切分，直到每个子块长度不超过max_tokens"""
        metadata: Dict[str, Any] = {}
        if node.title:
            metadata["title"] = node.title

        # 增加一层新的hierarchy层级标签，记录当前节点类型text/code/table
        hierarchy_paths: List[str] = []
        current_hierarchy = node.hierarchy.copy()
        max_level = 0
        for k in current_hierarchy.keys():
            if k.startswith("h") and k[1:].isdigit():
                level = int(k[1:])
                if level > max_level:
                    max_level = level

        new_level_key = f"h{max_level + 1}"
        type_value = node.node_type
        if node.node_type == "code":
            type_value = "code"
        elif node.node_type == "table":
            type_value = "table"
        elif node.node_type == "text":
            type_value = "text"
        current_hierarchy[new_level_key] = type_value
        hierarchy_paths = self._merge_node_hierarchies([Node(content="", hierarchy=current_hierarchy)])

        if hierarchy_paths:
            metadata["hierarchy"] = hierarchy_paths

        split_nodes: List[Node] = []
        if node.node_type == "code":
            split_nodes = self.block_splitter.split_code_block(node)
        elif node.node_type == "table":
            split_nodes = self.block_splitter.split_table_block(node)
        elif node.node_type == "text":
            split_nodes = self.block_splitter.split_text_block(node)

        group_id = f"{node.node_type}_{uuid4().hex}"
        total = len(split_nodes)
        docs: List[Document] = []
        for i, split_node in enumerate(split_nodes):
            final_metadata = self._update_metadata(
                content=split_node.content,
                base_metadata=metadata,
                has_incomplete=True,
                incomplete_type=node.node_type,
                complete_codes_count=0,
                complete_tables_count=0
            )
            final_metadata["total_chunks"] = total
            final_metadata["chunk_index"] = i + 1
            final_metadata["group_id"] = group_id
            docs.append(Document(page_content=split_node.content, metadata=final_metadata))

        return docs
        

    def split_file(self, file_content: str, path: Optional[str] = None) -> List[Document]:
        '''总流程：FrontMatter -> AST -> 展平 -> 贪心打包 -> 元数据收尾'''
        # 0. 提取 Front Matter
        front_matter, content_body = self._extract_front_matter(file_content)
        title = front_matter.get('title', '')
        
        # 1. 构建 AST
        # 如果存在标题，将其传递给元数据
        root_node = self._build_ast(content_body, title if title else None)
        
        # 2. 展平
        flat_nodes = self._flatten_tree(root_node)
        
        # 3. 贪心打包
        final_chunks = self._greedy_packing(flat_nodes)
        
        # 4. 后处理元数据
        for chunk in final_chunks:
            if path:
                chunk.metadata["path"] = path
            
        self._enrich_doc_with_hierarchy_text(chunk)
        return final_chunks
