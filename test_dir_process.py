import json
import os
from typing import List, Optional
from core import SmartMarkdownTreeSplitter


def process_directories(source_roots: List[str], output_root: str, config_path: str) -> None:
    """
    处理指定目录中的Markdown文件，生成块并保存到输出目录。
    """
    splitter = SmartMarkdownTreeSplitter(config_path=config_path)
    os.makedirs(output_root, exist_ok=True)

    total_files = 0
    total_chunks = 0

    for source_root in source_roots:
        if not os.path.isdir(source_root):
            print(f"警告: 目录不存在: {source_root}")
            continue

        for current_dir, _, files in os.walk(source_root):
            rel_dir = os.path.relpath(current_dir, os.path.join(os.path.dirname(config_path), "docs"))
            target_dir = os.path.join(output_root, rel_dir)
            os.makedirs(target_dir, exist_ok=True)

            for name in files:
                if not name.lower().endswith(".md"):
                    continue

                input_path = os.path.join(current_dir, name)
                try:
                    with open(input_path, "r", encoding="utf-8") as f:
                        text_content = f.read()
                except Exception as e:
                    print(f"读取文件 {input_path} 时出错: {e}")
                    continue

                doc_dir_path = current_dir.replace("\\", "/")
                chunks = splitter.split_file(text_content, path=doc_dir_path)
                total_files += 1
                total_chunks += len(chunks)

                output_path = os.path.join(target_dir, f"chunk_{os.path.splitext(name)[0]}.jsonl")

                with open(output_path, "w", encoding="utf-8") as out:
                    for chunk in chunks:
                        chunk_dict = {
                            "page_content": chunk.page_content,
                            "metadata": chunk.metadata
                        }
                        out.write(json.dumps(chunk_dict, ensure_ascii=False, indent=2) + "\n")

    print(f"处理完成 {total_files} 个文件")
    print(f"生成 {total_chunks} 个块")


def main() -> None:

    project_root = os.path.abspath(os.path.dirname(__file__))
    source_roots = [os.path.join(project_root, root) for root in ["docs/apis", "docs/components"]]
    output_root = os.path.join(project_root, "output")
    config_path = os.path.join(project_root, "config.yaml")

    process_directories(source_roots, output_root, config_path)


if __name__ == "__main__":
    main()
