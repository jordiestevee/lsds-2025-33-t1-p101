from pyspark import SparkContext

def is_even(number):
    return number % 2 == 0

def main():
    # Initialize SparkContext
    sc = SparkContext("local", "SumEvenNumbers")

    sc.setLogLevel("ERROR")

    # Read the file and split each line into numbers
    lines = sc.textFile("data/numbers2.txt")
    numbers = lines.flatMap(lambda line: [int(num) for num in line.split()])

    # Filter even numbers and compute their sum
    even_numbers = numbers.filter(is_even)
    sum_even = even_numbers.sum()

    # Print the result
    print(f"The sum of all even numbers is: {sum_even}")

    # Stop the SparkContext
    sc.stop()

if __name__ == "__main__":
    main()