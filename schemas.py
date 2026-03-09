from typing import List, Dict, Optional, Any
import yaml
from pydantic import BaseModel, Field

# --- Pydantic 配置模型 ---

class ChunkingRules(BaseModel):
    max_tokens: int = 800
    overlap_tokens: int = 50
    encoding_name: str = "cl100k_base"
    cross_header_merge_allowed: bool = True

class ElementProcessing(BaseModel):
    table_header_retention: bool = True
    enrich_enabled: bool = True


class SplitterConfig(BaseModel):
    chunking_rules: ChunkingRules
    element_processing: ElementProcessing

    @classmethod
    def from_yaml(cls, file_path: str):
        with open(file_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        return cls(**data)

# --- AST 节点类 ---

class Node:
    # 统一的中间节点结构：用于承载拆分前后内容与统计信息
    def __init__(
        self,
        content: str,
        node_type: str = "text",
        needs_split: bool = False,
        complete_codes_count: int = 0,
        complete_tables_count: int = 0,
        hierarchy: Dict[str, Any] = None,
        title: Optional[str] = None
    ):
        self.content = content
        self.node_type = node_type  # text, code, table, section
        self.needs_split = needs_split
        self.complete_codes_count = complete_codes_count
        self.complete_tables_count = complete_tables_count
        self.hierarchy = hierarchy if hierarchy is not None else {}
        self.title = title
        self.children: List['Node'] = []
        self._token_count = -1

    def add_child(self, child: 'Node'):
        self.children.append(child)

    def to_dict(self):
        return {
            "content": self.content[:50] + "...",
            "type": self.node_type,
            "needs_split": self.needs_split,
            "complete_codes_count": self.complete_codes_count,
            "complete_tables_count": self.complete_tables_count,
            "hierarchy": self.hierarchy,
            "title": self.title,
            "children": [c.to_dict() for c in self.children]
        }
