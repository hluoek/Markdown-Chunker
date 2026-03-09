from typing import List, Tuple, Dict, Any, Optional
from langchain_text_splitters import RecursiveCharacterTextSplitter, Language
from schemas import Node, SplitterConfig

class BlockSplitter:
    def __init__(self, config: SplitterConfig, tokenizer: Any):
        self.config = config
        self.tokenizer = tokenizer

    def _count_tokens(self, text: str) -> int:
        return len(self.tokenizer.encode(text))

    def split_text_block(self, node: Node) -> List[Node]:
        '''文本块切分，支持按中文和英文标点切分'''
        text_splitter = RecursiveCharacterTextSplitter(
            separators=[
                "\n\n", "\n",
                "。", "！", "？", "；",  # 加上中文分号
                ". ", "! ", "? ", "; ",
                "，", ", ", 
                " ",              
                ""                
            ],
            chunk_size=self.config.chunking_rules.max_tokens,
            chunk_overlap=self.config.chunking_rules.overlap_tokens,
            length_function=self._count_tokens,
            keep_separator=True
        )

        split_texts = text_splitter.split_text(node.content)

        nodes: List[Node] = []
        for text in split_texts:
            if len(text) <= 3:
                continue
            nodes.append(
                Node(
                    content=text,
                    node_type="text",
                    needs_split=False,
                    complete_codes_count=0,
                    complete_tables_count=0,
                    hierarchy=node.hierarchy.copy(),
                    title=node.title
                )
            )
        return nodes

    def split_code_block(self, node: Node) -> List[Node]:
        '''代码块切分，支持按语言切分'''
        lines = node.content.split('\n')
        lang_str = lines[0].strip().removeprefix("```").strip() or "text"
        inner_code = "\n".join(lines[1:-1])

        # 尝试映射到 LangChain 支持的 Language 枚举
        lc_lang = self._get_langchain_language(lang_str)
        
        # 使用 RecursiveCharacterTextSplitter 进行切分
        splitter = RecursiveCharacterTextSplitter.from_language(
            language=lc_lang,
            chunk_size=self.config.chunking_rules.max_tokens,
            chunk_overlap=self.config.chunking_rules.overlap_tokens,
            length_function=self._count_tokens
        )
        
        # 切分代码内容
        split_texts = splitter.split_text(inner_code)
        
        # 将切分后的文本块重新封装为 Node
        nodes: List[Node] = []
        for text in split_texts:
            wrapped_content = f"```{lang_str}\n{text}\n```"
            nodes.append(
                Node(
                    content=wrapped_content,
                    node_type="code",
                    needs_split=False,
                    complete_codes_count=1,
                    complete_tables_count=0,
                    hierarchy=node.hierarchy.copy(),
                    title=node.title
                )
            )
        return nodes

    def _get_langchain_language(self, lang: str) -> Language:
        """将字符串语言名称映射到 LangChain 的 Language 枚举。"""
        lang_map = {
            "python": Language.PYTHON,
            "javascript": Language.JS,
            "js": Language.JS,
            "typescript": Language.JS,
            "ts": Language.JS,
            "tsx": Language.JS,
            "java": Language.JAVA,
            "cpp": Language.CPP,
        }
        
        normalized_lang = lang.lower()
        if normalized_lang in lang_map:
            return lang_map[normalized_lang]
        
        # 尝试直接匹配 Enum name
        try:
            return Language[normalized_lang.upper()]
        except KeyError:
            raise ValueError(f"Unsupported language for LangChain splitter: {lang}")

    def split_table_block(self, node: Node) -> List[Node]:
        '''表格切分时重复保留前两行（表头 + 分隔线），保证子块可独立理解'''
        lines = node.content.split('\n')
        if len(lines) < 3:
            return [
                Node(
                    content=node.content,
                    node_type="table",
                    needs_split=False,
                    complete_codes_count=0,
                    complete_tables_count=0,
                    hierarchy=node.hierarchy.copy(),
                    title=node.title
                )
            ]
            
        header_lines = lines[:2]
        body_lines = lines[2:]
        
        header_text = "\n".join(header_lines)
        header_tokens = self._count_tokens(header_text)
        
        nodes: List[Node] = []
        current_lines = []
        current_tokens = header_tokens
        max_tokens = self.config.chunking_rules.max_tokens
        
        for line in body_lines:
            line_tokens = self._count_tokens(line) + 1
            
            if current_tokens + line_tokens > max_tokens and current_lines:
                content = header_text + "\n" + "\n".join(current_lines)
                nodes.append(
                    Node(
                        content=content,
                        node_type="table",
                        needs_split=False,
                        complete_codes_count=0,
                        complete_tables_count=0,
                        hierarchy=node.hierarchy.copy(),
                        title=node.title
                    )
                )
                current_lines = [line]
                current_tokens = header_tokens + line_tokens
            else:
                current_lines.append(line)
                current_tokens += line_tokens
                
        if current_lines:
            content = header_text + "\n" + "\n".join(current_lines)
            nodes.append(
                Node(
                    content=content,
                    node_type="table",
                    needs_split=False,
                    complete_codes_count=0,
                    complete_tables_count=0,
                    hierarchy=node.hierarchy.copy(),
                    title=node.title
                )
            )
            
        return nodes
