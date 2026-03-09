import json
import os
from core import SmartMarkdownTreeSplitter


def process_single_file(input_path: str, output_dir: str, config_path: str) -> None:
    """
    处理单个Markdown文件，生成块并保存到输出目录。
    """
    if not os.path.exists(input_path):
        print(f"错误: 输入文件不存在: {input_path}")
        return

    splitter = SmartMarkdownTreeSplitter(config_path=config_path)

    try:
        with open(input_path, "r", encoding="utf-8") as f:
            text_content = f.read()
    except Exception as e:
        print(f"读取文件 {input_path} 时出错: {e}")
        return

    doc_dir_path = os.path.dirname(input_path).replace("\\", "/")
    chunks = splitter.split_file(text_content, path=doc_dir_path)

    os.makedirs(output_dir, exist_ok=True)
    
    filename = os.path.basename(input_path)
    output_name = f"chunk_{os.path.splitext(filename)[0]}.jsonl"
    output_path = os.path.join(output_dir, output_name)

    with open(output_path, "w", encoding="utf-8") as out:
        for chunk in chunks:
            chunk_dict = {
                "page_content": chunk.page_content,
                "metadata": chunk.metadata
            }
            out.write(json.dumps(chunk_dict, ensure_ascii=False, indent=2) + "\n")

    print(f"输入文件: {input_path}")
    print(f"输出文件: {output_path}")
    print(f"生成 {len(chunks)} 个块")


def main() -> None:
    project_root = os.path.abspath(os.path.dirname(__file__))
    input_path = os.path.join(project_root, "test_doc.md")
    output_dir = os.path.join(project_root, "output")
    config_path = os.path.join(project_root, "config.yaml")

    process_single_file(input_path, output_dir, config_path)


if __name__ == "__main__":
    main()
