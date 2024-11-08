import numpy as np
import sys

def generate_zipf_like_distribution(n, a, size):
    """Generate Zipf-like distribution for exponent a <= 1."""
    ranks = np.arange(1, n+1)
    weights = ranks ** (-a)
    weights /= weights.sum()  # Normalize to make probabilities sum to 1
    return np.random.choice(ranks, size=size, p=weights)

# Check for correct number of arguments
if len(sys.argv) != 4:
    print("Usage: python3 select_zipf.py <input_file> <number_of_lines_to_select> <exponent_a>")
    sys.exit(1)

# Get the input parameters
input_file = sys.argv[1]
lines_to_select = int(sys.argv[2])
exponent = float(sys.argv[3])

# Read the file lines into a list
try:
    with open(input_file, 'r') as f:
        lines = f.readlines()
except FileNotFoundError:
    print(f"File not found: {input_file}")
    sys.exit(1)

# Get the total number of lines in the file
total_lines = len(lines)

# Generate the custom Zipf-like distribution
if exponent > 1:
    zipf_indices = np.random.zipf(exponent, lines_to_select)
else:
    zipf_indices = generate_zipf_like_distribution(total_lines, exponent, lines_to_select)

# Ensure the indices are within valid range (1-based index)
zipf_indices = np.clip(zipf_indices, 1, total_lines)

# Select and print the corresponding lines
#print("Selected lines based on Zipf-like distribution:")
for index in zipf_indices:
    print(lines[index - 1].strip())  # -1 because of 0-based indexing in Python

