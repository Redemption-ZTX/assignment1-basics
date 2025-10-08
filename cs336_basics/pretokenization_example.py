
import os
from typing import BinaryIO
import regex as re
from multiprocessing import Pool


PAT=r"""'(?:[sdmt]|ll|ve|re)| ?\p{L}+| ?\p{N}+| ?[^\s\p{L}\p{N}]+|\s+(?!\S)|\s{1,1}"""


def process_chunk(args):
    """在子进程中完成读取、预分词和频率统计"""
    filename, start, end, split_special_token = args
    
    # 读取文件块
    with open(filename, "rb") as file:
        file.seek(start)
        chunk = file.read(end - start).decode("utf-8", errors="ignore")
    
    # 移除 special token
    delimiter = split_special_token.decode('utf-8')
    chunk_without_special = chunk.replace(delimiter, '')
    
    # 预分词（使用 finditer 节省内存）
    matches = re.finditer(PAT, chunk_without_special)
    
    # 统计每个 token 的频率（作为 bytes tuple）
    word_counts = {}
    for match in matches:
        token = match.group()
        
        # string -> bytes -> tuple of individual bytes
        byte_string = token.encode('utf-8')
        byte_tuple = tuple(bytes([b]) for b in byte_string)
        
        # 统计频率
        if byte_tuple in word_counts:
            word_counts[byte_tuple] += 1
        else:
            word_counts[byte_tuple] = 1
    
    return word_counts


def find_chunk_boundaries(
    file: BinaryIO,
    desired_num_chunks: int,
    split_special_token: bytes,
) -> list[int]:
    """
    Chunk the file into parts that can be counted independently.
    May return fewer chunks if the boundaries end up overlapping.
    """
    assert isinstance(split_special_token, bytes), "Must represent special token as a bytestring"

    # Get total file size in bytes
    file.seek(0, os.SEEK_END)
    file_size = file.tell()
    file.seek(0)

    chunk_size = file_size // desired_num_chunks

    # Initial guesses for chunk boundary locations, uniformly spaced
    # Chunks start on previous index, don't include last index
    chunk_boundaries = [i * chunk_size for i in range(desired_num_chunks + 1)]
    chunk_boundaries[-1] = file_size

    mini_chunk_size = 4096  # Read ahead by 4k bytes at a time

    for bi in range(1, len(chunk_boundaries) - 1):
        initial_position = chunk_boundaries[bi]
        file.seek(initial_position)  # Start at boundary guess
        while True:
            mini_chunk = file.read(mini_chunk_size)  # Read a mini chunk

            # If EOF, this boundary should be at the end of the file
            if mini_chunk == b"":
                chunk_boundaries[bi] = file_size
                break

            # Find the special token in the mini chunk
            found_at = mini_chunk.find(split_special_token)
            if found_at != -1:
                chunk_boundaries[bi] = initial_position + found_at
                break
            initial_position += mini_chunk_size

    # Make sure all boundaries are unique, but might be fewer than desired_num_chunks
    return sorted(set(chunk_boundaries))

def get_chunks(path: str, num_processes: int, split_special_token: bytes) -> dict:
    """
    并行处理大文件，返回所有预分词tokens的频率统计
    
    Args:
        path: 文件路径
        num_processes: 并行进程数
        split_special_token: 分隔符（如 b"<|endoftext|>"）
    
    Returns:
        dict[tuple[bytes], int]: 每个 token 的频率
    """
    with open(path, "rb") as f:
        boundaries = find_chunk_boundaries(f, num_processes, split_special_token)
    
    # 并行处理每个块
    with Pool(num_processes) as p:
        # 准备任务参数：每个任务包含 split_special_token
        tasks = [(path, start, end, split_special_token) 
                 for start, end in zip(boundaries[:-1], boundaries[1:])]
        
        # 并行执行，每个子进程返回一个 word_counts 字典
        chunk_word_counts_list = p.map(process_chunk, tasks)
    
    # 合并所有子进程的统计结果
    word_counts = {}
    for chunk_word_counts in chunk_word_counts_list:
        for word, freq in chunk_word_counts.items():
            if word in word_counts:
                word_counts[word] += freq
            else:
                word_counts[word] = freq
    
    return word_counts

## Usage
# if __name__ == "__main__":
#     filename = "/cephfs/zhaotianxiang/backup/cs336/assignment1-basics/data/TinyStoriesV2-GPT4-valid.txt"
#     num_processes = 16
#     split_special_token = b"<|endoftext|>"
    
#     # 第一步：找到文件块边界
#     with open(filename, "rb") as f:
#         boundaries = find_chunk_boundaries(f, num_processes, split_special_token)
    
#     print(f"找到 {len(boundaries)-1} 个块")
#     print(f"边界: {boundaries}\n")
    
#     # 第二步：并行处理每个块
#     with Pool(num_processes) as p:
#         # 准备参数：[(filename, start1, end1), (filename, start2, end2), ...]
#         tasks = [(filename, start, end) for start, end in zip(boundaries[:-1], boundaries[1:])]
        
#         # 使用 starmap 来传递多个参数
#         results = p.starmap(process_chunk, tasks)
#         # 显示结果
#         total_stories = 0
#         total_tokens = 0
#         for i, chunk in enumerate(results):
#             delimiter = "<|endoftext|>"
            
#             # 分割成故事列表
#             stories = chunk.split(delimiter)
#             # 过滤掉空字符串
#             stories = [s.strip() for s in stories if s.strip()]

#             chunk_rejoined = " ".join(stories)
            
            
#             # 对整个 chunk 进行分词（使用原始 chunk，不是 stories 列表）
#             matches = re.findall(PAT, chunk_rejoined)
#             print(matches)
#             # print(f"前10个tokens: {matches[:10]}")  # 可选：查看前几个token
            
#             total_stories += len(stories)
#             total_tokens += len(matches)
            
#             print(f"块 {i+1}:")
#             print(f"  字符数: {len(chunk)}")
#             print(f"  故事数: {len(stories)}")
#             print(f"  Token数: {len(matches)}")
#             print(f"  平均每故事Token数: {len(matches)//len(stories) if stories else 0}")
#             print(f"  第一个故事预览: {stories[0][:150]}..." if stories else "  (空)")
#             print()
        
#         print(f"=" * 50)
#         print(f"总故事数: {total_stories}")
#         print(f"总Token数: {total_tokens}")
#         print(f"平均每故事Token数: {total_tokens//total_stories if total_stories else 0}")
    
#     print("处理完成！")


if __name__ == "__main__":
    path = "/cephfs/zhaotianxiang/backup/cs336/assignment1-basics/data/TinyStoriesV2-GPT4-valid.txt"
    num_processes = 16
    split_special_token = b"<|endoftext|>"
    
    print(f"开始处理文件: {path}")
    print(f"使用 {num_processes} 个进程并行处理\n")
    
    tokens = get_chunks(path, num_processes, split_special_token)
    
    print(f"\n{'='*50}")
    print(f"处理完成！")
    print(f"总Token数: {len(tokens):,}")
    print(f"前10个token示例:")
    for token, count in list(tokens.items())[:10]:
        print(f"  {token}: {count}")
