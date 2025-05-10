import requests
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
import matplotlib.pyplot as plt


def map_function(word):
    return word, 1


def shuffle_function(mapped_values):
    shuffled = defaultdict(list)
    for key, value in mapped_values:
        shuffled[key].append(value)
    return shuffled.items()


def reduce_function(key_values):
    key, values = key_values
    return key, sum(values)


def map_reduce(text):
    """
    Perform MapReduce on the given text to calculate word frequencies.
    """
    words = text.split()

    # Parallel Mapping
    with ThreadPoolExecutor() as executor:
        mapped_values = list(executor.map(map_function, words))

    # Shuffle Step
    shuffled_values = shuffle_function(mapped_values)

    # Parallel Reduction
    with ThreadPoolExecutor() as executor:
        reduced_values = list(executor.map(reduce_function, shuffled_values))

    return dict(reduced_values)


def visualize_top_words(word_counts, top_n=10):
    """
    Visualize the top N words and their frequencies using a bar chart.
    """
    # Sort words by frequency in descending order
    sorted_words = sorted(word_counts.items(), key=lambda x: x[1], reverse=True)[:top_n]

    # Extract words
    words, counts = zip(*sorted_words)

    # Create a bar chart
    plt.figure(figsize=(10, 6))
    plt.bar(words, counts, color="skyblue")
    plt.xlabel("Words")
    plt.ylabel("Frequency")
    plt.title(f"Top {top_n} Words by Frequency")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    # URL to fetch the text from
    url = "https://www.gutenberg.org/files/1342/1342-0.txt"

    print("Downloading text from URL...")
    response = requests.get(url)
    if response.status_code == 200:
        text = response.text
        print("Text downloaded successfully.")
    else:
        print(f"Failed to download text. HTTP Status Code: {response.status_code}")
        exit(1)

    # Perform MapReduce
    print("Performing MapReduce...")
    word_counts = map_reduce(text)

    # Visualize the top 10 words
    print("Visualizing top 10 words...")
    visualize_top_words(word_counts, top_n=10)
