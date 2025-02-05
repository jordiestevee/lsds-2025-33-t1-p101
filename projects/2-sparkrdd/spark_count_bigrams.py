from pyspark import SparkContext

def create_bigrams(line):
    words = line.split()  # Split the line into words
    bigrams = zip(words, words[1:])  # Create bigrams (pairs of consecutive words)
    return bigrams

def main():
    # Initialize SparkContext
    sc = SparkContext("local", "Bigram Count")

    # Read the input file
    lines = sc.textFile("data/cat.txt")

    # Create bigrams and count them
    bigrams = lines.flatMap(create_bigrams)  # Flatten the list of bigrams
    bigram_counts = bigrams.map(lambda bigram: (bigram, 1)).reduceByKey(lambda a, b: a + b)  # Count bigrams

    # Collect and print the results
    results = bigram_counts.collect()
    for bigram, count in results:
        print(f"{bigram}: {count}")

    # Stop the SparkContext
    sc.stop()

if __name__ == "__main__":
    main()