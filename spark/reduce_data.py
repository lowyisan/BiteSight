import sys

def truncate_tsv(input_file, output_file, max_lines=10000):
    """
    Reads 'input_file' and writes only the first 'max_lines' lines to 'output_file'.
    """
    with open(input_file, 'r', encoding='utf-8') as f_in, open(output_file, 'w', encoding='utf-8') as f_out:
        for i, line in enumerate(f_in):
            if i >= max_lines:
                break
            f_out.write(line)

if __name__ == "__main__":
    # Example usage from the command line:
    # python truncate_tsv.py input.tsv output.tsv 10000
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} input_tsv output_tsv [max_lines=10000]")
        sys.exit(1)

    input_tsv = sys.argv[1]
    output_tsv = sys.argv[2]
    max_lines = int(sys.argv[3]) if len(sys.argv) > 3 else 10000

    truncate_tsv(input_tsv, output_tsv, max_lines)
    print(f"Truncated '{input_tsv}' to {max_lines} lines in '{output_tsv}'")