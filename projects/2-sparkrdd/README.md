# Analyzing Tweets using Spark RDD

The goal of this lab is to use Spark RDDs to analyze a large volume of Tweets in a Spark cluster.

# Table of contents

- [Required exercises](#required-exercises)
    - [Lab 3: Downloading Tweets from S3 and parsing them from JSON](#lab-3-downloading-tweets-from-s3-and-parsing-them-from-json)
    - [Seminar 3: Using Spark RDDs](#seminar-3-using-spark-rdds)
    - [Lab 4: Analyzing Tweets with Spark](#lab-4-analyzing-tweets-with-spark)
    - [Seminar 4: Running Spark in AWS](#seminar-4-running-spark-in-aws)
- [Additional exercises](#additional-exercises)

# Required exercises

Remember you must format your code with black and follow PEP8 conventions.

## Lab 3: Parsing Tweets as JSON

### [L3Q0] [5 marks] The tweets dataset

- Follow the Developer Setup to download the needed data if you did not at the beginning of the course.
- Take a look at the first Tweet: `cat Eurovision3.json -n | head -n 1 | jq`. [Help](https://unix.stackexchange.com/questions/288521/with-the-linux-cat-command-how-do-i-show-only-certain-lines-by-number#:~:text=cat%20%2Fvar%2Flog%2Fsyslog%20-n%20%7C%20head%20-n%2050%20%7C,-b10%20-a10%20will%20show%20lines%2040%20thru%2060.)
- **[1 mark]** What field in the JSON object of a Tweet contains the user bio?

  The user bio is contained in the **"description"** field within the **"user"** object of the JSON.  

For example, in the first tweet:  
```json
"user": {
  ...
  "description": "no todo lo que brilla es oro, a veces es highlight ✨💜"
}
```

- **[1 mark]** What field in the JSON object of a Tweet contains the language?

  The language of the tweet is contained in the **"lang"** field of the JSON object.  

For example:  
```json
"lang": "es"
```  
This indicates that the tweet is in Spanish.

- **[1 mark]** What field in the JSON object of a Tweet contains the text content?

  The text content of the tweet is contained in the **"text"** field of the JSON object.  

For example:  
```json
"text": "RT @carloscarmo98: -Manel, algo que decir sobre tu actuación en Eurovision?\n-Kikiriketediga https://t.co/yXGYtKmJoM"
```

- **[1 mark]** What field in the JSON object of a Tweet contains the number of followers?

  The number of followers is contained in the **"followers_count"** field within the **"user"** object of the JSON.  

For example:  
```json
"user": {
  ...
  "followers_count": 718
}
```  

- Take a look at the first two lines: `cat Eurovision3.json -n | head -n 2`.
- **[1 mark]** How many Tweets does each line contain?

Each of the two lines contains **one tweet**. The JSON data for each line corresponds to a single tweet.

### [L3Q1] [5 marks] Parsing JSON with Python

- Create a file `tweet_parser.py`
- Create a `Tweet` dataclass with fields for the `tweet_id` (int), `text` (str), `user_id` (int), `user_name` (str), `language` (str), `timestamp_ms` (int), `retweeted_id` (int, the id of the retweeted tweet or None) and `retweeted_user_id` (int, the id of the retweeted tweet's user or None). [Help](https://realpython.com/python-data-classes/)
- Create a function `parse_tweet(tweet: str) -> Tweet` that takes in a Tweet as a Json string and returns a Tweet object. [Help](https://stackoverflow.com/a/7771071)
- Read the first line of `Eurovision3.json` and print the result of `parse_tweet`. [Help](https://stackoverflow.com/questions/1904394/read-only-the-first-line-of-a-file)
- Take a screenshot and add it to the README.
- Push your changes.

  ![image](https://github.com/user-attachments/assets/795e182c-03b6-4bc5-802f-9fb6298d70b5)


### [L3Q2] [5 marks] Counting Tweets by language

- Create a file `simple_tweet_language_counter.py`
- Implement a script that reads each line of `Eurovision3.json` one by one. [Help](https://stackoverflow.com/a/3277512)
    - You might need to skip any invalid lines, such as empty lines with only a `\n` or Tweets with an invalid JSON format.
- Parse each Tweet using the `parse_tweet` function from the previous exercise.
- Count the number of Tweets of each language using a dictionary. [Help](https://www.w3schools.com/python/python_dictionaries.asp)
- Print the dictionary. Take a screenshot and add it to the README.
- Push your changes.

![image](https://github.com/user-attachments/assets/9209fa06-f52b-4313-b978-270f8d0c152c)


## Seminar 3: Using Spark RDDs

> Before starting this section, read [RDD Programming Guide](https://spark.apache.org/docs/latest/rdd-programming-guide.html) and [Resilient Distributed Datasets](https://www.usenix.org/system/files/conference/nsdi12/nsdi12-final138.pdf)

### [S3Q0] [10 marks] What is Spark RDD?

- **[1 mark]** What is the difference between a transformation and an action?

 **Transformation**: A transformation in Spark is an operation that creates a new RDD from an existing one. Transformations are lazy, meaning they are not executed 
 immediately but only when an action is triggered. Examples: map(), filter(), flatMap().
 
 **Action**: An action triggers the execution of transformations and returns a result to the driver or writes data to an external system. Examples: collect(), count(), 
 reduce().

  
- **[1 mark]** What is the difference between a wide and a narrow dependency? What is a stage in Spark RDD?

  **Narrow Dependency:** In a narrow dependency, each partition of the child RDD depends on at most one partition of the parent RDD. Example: map(), filter().
  
  **Wide Dependency:** In a wide dependency, a partition of the child RDD depends on multiple partitions of the parent RDD, often requiring a shuffle across nodes. 
   Example: groupByKey(), reduceByKey().
  
- Start up a Spark cluster locally using Docker compose: `docker-compose up`.
- **[1 mark]** How many Spark workers exist in your local cluster? Take a screenshot of Docker Desktop and add it to the README.
  
  ![image](https://github.com/user-attachments/assets/49cc0232-c04b-4569-85b1-38b5d8d55098)

  As we can see, there exist 2 spark workers in our local cluster

- **[3 mark]** What is a lambda function in Python?

      A lambda function in Python is a small, anonymous function defined using the lambda keyword. It can take any number of arguments but can only have one expression.         The result of the expression is automatically returned.
  
- **[3 mark]** What do the RDD operations `map`, `filter`, `groupByKey` and `flatMap` do?
  
      1. map
        Purpose: Applies a function to each element of the RDD and returns a new RDD of the results.

        Input: A function that takes one element and returns one transformed element.

        Output: A new RDD where each element is the result of applying the function.

      2. filter
        Purpose: Selects elements from the RDD that satisfy a condition (predicate).

        Input: A function that takes one element and returns True or False.

        Output: A new RDD containing only the elements that satisfy the condition.

      3. groupByKey
        Purpose: Groups the values for each key in a key-value pair RDD.

        Input: An RDD of key-value pairs (K, V).

        Output: An RDD of (K, Iterable<V>) pairs, where all values for the same key are grouped into an iterable.

      4. flatMap
        Purpose: Applies a function to each element of the RDD and flattens the result into a single list.

        Input: A function that takes one element and returns an iterable (e.g., a list).

        Output: A new RDD where each input element is expanded into zero or more output elements.

  
- Check the local IP for the Spark Master service in the `spark-master-1` container logs. You should see a log similar to `Starting Spark master at spark://172.20.0.2:7077`.
- Run the job with Spark: `docker-compose exec spark-master spark-submit --master spark://{IP_FRM_PREVIOUS_STEP}:7077 /opt/bitnami/spark/app/spark_sum.py /opt/bitnami/spark/app/data/numbers1.txt`
- **[1 mark]** Take a close look at the logs. What was the result of your job?
- The result is 55

- ![image](https://github.com/user-attachments/assets/3486ed0c-f133-4dd1-9916-713f4c144ece)


### [S3Q1] [5 marks]  Sum the numbers

The file [numbers2.txt](./data/numbers2.txt) has many lines, each with many numbers.

- Create a file `spark_sum2.py`
- Implement and run a Spark job that computes the sum of all the numbers.
- Write the command you used to run it in the README and show a screenshot of the result.
- The result is 195, and the command is: docker-compose exec spark-master spark-submit --master spark://172.21.0.2:7077 /opt/bitnami/spark/app/spark_sum2.py /opt/bitnami/spark/app/data/numbers2.txt
  ![image](https://github.com/user-attachments/assets/466c3052-a311-4145-8fba-80d5e7a861f0)


### [S3Q2] [5 marks] Sum the even numbers

The file [numbers2.txt](./data/numbers2.txt) has many lines, each with many numbers.

- Create a file `spark_sum3.py`
- Implement and run a Spark job that computes the sum of all the even numbers.
- Write the command you used to run it in the README and show a screenshot of the result.
- - The result is 100, and the command is: docker-compose exec spark-master spark-submit --master spark://172.19.0.2:7077 /opt/bitnami/spark/app/spark_sum3.py /opt/bitnami/spark/app/data/numbers2.txt

  ![image](https://github.com/user-attachments/assets/9499e4bc-b449-46d3-81d0-2bcb7d984485)


### [S3Q3] [5 marks] Find how many people live in each city

The file [people.txt](./data/people.txt) has many lines, each with `{NAME} {LANGUAGE} {CITY}`.

- Create a file `spark_count_people.py`
- Implement and run a Spark job that counts how many people live in each city.
- Write the command you used to run it in the README and show a screenshot of the result.
- The command is: docker-compose exec spark-master spark-submit --master spark://172.21.0.2:7077 /opt/bitnami/spark/app/spark_count_people.py /opt/bitnami/spark/app/data/people.txt
  ![image](https://github.com/user-attachments/assets/d00963ce-a9dc-4179-98d9-89aa5b27ca36)


### [S3Q4] [5 marks] Count the bigrams

The file [cat.txt](./data/cat.txt) has many lines, each with a sentence.

- Create a file `spark_count_bigrams.py`
- Implement and run a Spark job that counts how many people live in each city.
- Write the command you used to run it in the README and show a screenshot of the result.
- The command is:  docker-compose exec spark-master spark-submit --master spark://172.19.0.2:7077 /opt/bitnami/spark/app/spark_count_bigrams.py /opt/bitnami/spark/app/data/cat.txt

  ![image](https://github.com/user-attachments/assets/4b928e2c-a0b2-4221-a192-9b85ada45dc6)


## Lab 4: Analyzing Tweets with Spark

### [L4Q0] [10 marks] Filtering Tweets by language with Spark

- Create a file `spark_tweet_language_filter.py`.
- Implement a Spark job that finds all the tweets in a file for a given language (e.g. `zh`)
- Saves the result to a file
- Run your code in your local Spark cluster:
```zsh
docker-compose exec spark-master spark-submit --master spark://{IP_FROM_PREVIOUS_STEP}:7077 /opt/bitnami/spark/app/spark_tweet_language_filter.py zh /opt/bitnami/spark/app/data/Eurovision3.json /opt/bitnami/spark/output/Eurovision3Zh.json
```

> You might need to `chmod 755 data` if you get "file not found" errors


### [L4Q1] [10 marks] Get the most repeated bigrams

- Create a file `spark_tweet_bigrams.py`.
- Implement a Spark job that finds the most repeated bigrams for a language (e.g. `es`)
- Filter out bigrams that only appear once
- Saves the result to a file (sorted by how many times they appear in descending order)
- Run your code in your local Spark cluster:
```zsh
docker-compose exec spark-master spark-submit --master spark://{IP_FROM_PREVIOUS_STEP}:7077 /opt/bitnami/spark/app/spark_tweet_bigrams.py es /opt/bitnami/spark/app/data/Eurovision3.json /opt/bitnami/spark/output/Eurovision3EsBigrams
```


### [L4Q2] [10 marks] Get the 10 most retweeted tweets

- Create a file `spark_tweet_retweets.py`.
- Implement a Spark job that finds the users with the top 10 most retweeted Tweets for a language
- Run your code in your local Spark cluster:
```zsh
docker-compose exec spark-master spark-submit --master spark://{IP_FROM_PREVIOUS_STEP}:7077 /opt/bitnami/spark/app/spark_tweet_retweets.py es /opt/bitnami/spark/app/data/Eurovision3.json
```

### [L4Q3] [10 marks] Get the 10 most retweeted users

- Create a file `spark_tweet_user_retweets.py`.
- Implement a Spark job that finds the users with the top 10 most retweets (in total) for a language and how many retweets they have. I.e., sum all the retweets each user has and get the top 10 users.
- Run your code in your local Spark cluster:
```zsh
docker-compose exec spark-master spark-submit --master spark://{IP_FROM_PREVIOUS_STEP}:7077 /opt/bitnami/spark/app/spark_tweet_user_retweets.py es /opt/bitnami/spark/app/data/Eurovision3.json
```

## Seminar 4: Running Spark in AWS

AWS allows us to rent virtual servers and deploy a Spark cluster to do data anlysis at scale. In this seminar, you will learn how to:
- Use S3 to store and read files
- Use AWS EMR to host a Spark cluster in AWS EC2 servers
- Run some of your Spark applications in the cluster.

### [S4Q0] [10 marks] Run L4Q1 in AWS using EMR

- Accept the invitation to AWS academy.
- Open the [AWS Academy](https://awsacademy.instructure.com/courses) course
- In `Modules`, select `Launch AWS Academy Learner Lab`
- Click `Start Lab`
- Wait until the `AWS` indicator has a green circle
- Click the `AWS` text with the green circle to open the AWS console

> [!TIP]
> When you launch a cluster, you start spending AWS credit! Remember to terminate your cluster at the end of your experiments!

- [Create a bucket in S3](https://us-east-1.console.aws.amazon.com/s3/home?region=us-east-1#):
    - Bucket type: `General purpose`
    - Name: `lsds-2025-{group_number}-t{theory_number}-p{lab_number}-s{seminar_number}-s3bucket`

- Paste a screenshot

![image](https://github.com/user-attachments/assets/7dd05f80-61c7-4146-bfb8-1e3ed74da57b)


- In the bucket, create 4 folders: `input`, `app`, `logs` and `output`

- Paste a screenshot

![image](https://github.com/user-attachments/assets/7ab7bf32-d9c9-4f12-a1af-f3c1590543a5)

- Upload the `Eurovision3.json` file inside the `input` folder

- Paste a screenshot

 ![image](https://github.com/user-attachments/assets/64913dd2-99b6-4daa-80f9-f2532c0d6ad9)


- Upload `spark_tweet_user_retweets.py` and `tweet_parser.py` in the `app` folder

- Paste a screenshot

  ![image](https://github.com/user-attachments/assets/6d09ea35-dfad-48d9-b512-d3f8c41b74cb)


- Open the [EMR console](https://us-east-1.console.aws.amazon.com/emr/home?region=us-east-1#/clusters)

- Create a cluster
    - Application bundle: `Spark Interactive`
    - Name: `lsds-2025-{group_number}-t{theory_number}-p{lab_number}-s{seminar_number}-sparkcluster`
    - Choose this instance type: `m4.large`
    - Instance(s) size: `3`
    - Cluster logs: select the `logs` folder in the S3 bucket you created
    - Service role: `EMR_DefaultRole`
    - Instance profile: `EMR_EC2_DefaultRole`
    
- Paste a screenshot

![image](https://github.com/user-attachments/assets/31caa60c-9c7d-4262-87b3-7fa3a9f42dcb)



- In `Steps`, select `Add step`.
    - Type: `Spark application`
    - Name: `lab2-ex13`
    - Deploy mode: `Cluster mode`
    - Application location: select the `spark_tweet_user_retweets.py` in the S3 bucket
    - Spark-submit options: specify the `tweet_parser.py` module. For example: `--py-files s3://lsds-2025-miquel-test/app/tweet_parser.py`
    - Arguments: specify the input and output. For example: `es s3://lsds-2025-miquel-test/input/Eurovision3.json`.

- Paste a screenshot

![image](https://github.com/user-attachments/assets/e5c24ee3-a032-4972-918e-4d21ca64c866)


- When you submit a step, wait until the `Status` is `Completed`. 

- Paste a screenshot

![image](https://github.com/user-attachments/assets/1a41bbd8-f9f7-4935-94fe-43a2678b9ef5)


> [!TIP]
> You can find the logs in your S3 bucket: `logs/{cluster id}/containers/application_*_{run number}/container_*_000001/stdout.gz` - they might take some minutes to appear

- Paste a screenshot of the log where we can see: how much time it took, what are the ids of the ten most retweeted users.

![image](https://github.com/user-attachments/assets/f8a20312-01e6-452e-b718-623d077a8e11)

Note: I tried to follow your instructions of the logs part but at the time of opening the file stdout.gz in the bucket an error occurs with the file tweet_parser.py, and I haven't been able to fix it at time to deliver.

# Additional exercises

You can earn an additional 2 marks (over 10) on this project's grade by working on additional exercises. To earn the full +2, you need to complete 4 additional exercises. 

During these exercises, you will build a (super simple) search engine, like a barebones Google.

### [AD1Q0] Crawling

Find the latest available Wikipedia datasets from [dumps.wikimedia](https://dumps.wikimedia.org/other/enterprise_html/runs/). For example, `https://dumps.wikimedia.org/other/enterprise_html/runs/20240901/enwiki-NS0-20240901-ENTERPRISE-HTML.json.tar.gz`.

Then, download the first 10, 100, 1k, 10k and 100k articles in different files. The smaller datasets will be useful for testing (replace `$1` with how many articles you want to download).

```zsh
curl -L https://dumps.wikimedia.org/other/enterprise_html/runs/20240901/enwiki-NS0-20240901-ENTERPRISE-HTML.json.tar.gz | tar xz --to-stdout | head -n $1 > wikipedia$1.json
```

Paste the first Wikipedia article here, properly formatted as JSON.

### [AD1Q1] Building the repository

Write a Python or bash script that splits the big file into multiple files, one file per line. The file name should be the identifier of the article, and the content of the file the full JSON object.

Run said script for the 10 and 1k datasets.


### [AD1Q2] Building the reverse index

Write a Spark RDD job that creates a reverse index for all the crawled articles.

The reverse index must map every word in the abstract of every article, to the list of article (ids) that contain it. Store this as a file. The format must be: `LINE CRLF LINE CRLF LINE CRLF ...`, where each `LINE` is `WORD SP DOCID SP DOCID SP DOCID SP ... DOCID`. For example:

```
seven 18847712 76669474 76713187 75388615 1882504 18733291 19220717 3118126 31421710 26323888 52867888 76712306 76711442 48957757
seasons 58765506 76669474 7755966 66730851 53056676 40360169 7871468 60331788 52867888 70406270 52243132 17781886
22 12000256 14177667 56360708 50648266 31581711 76395922 31418962 73082202 33375130 76669474 76713187 5799657 40360169 65704112 18688178 48850419 37078259 63141238 40538167 32644089
due 76731844 41098246 25214406 41098253 1658830 31581711 8905616 45711377 14259409 76708884 2723548 76732829 1122974 41233503 43331165 76669474 12365159 18733291 7871468 65704112 63447415 63840761 68538426 36367677
sold 76669474 31728882 53538197 63141238 12243595
```

Remember to strip all symbols and make the text lowercase before indexing. For example: `hello`, `HELLO`, `HeLLo`, `hello,`, `hello?`, `[hello]` and `hello!` must all be treated as the same word.

Customize the partitioner function to be `ord(key[0]) % PARTITION_COUNT`, such that we can easily know in which partition a word will be in the inverse index. Make `PARTITION_COUNT` a parameter. Verify that the words are indeed in the correct partition.

Test it locally with the 10 and 100 datasets.

### [AD1Q3] Build a search API

Create a FastAPI service that, for a given query of space-separated words, returns the name, abstract, identifier and URL of all Wikipedia articles that contain all those words.

The API should look like this:

```
curl -X POST localhost:8080/search -H "Content-Type: application/json" -d '{
    "query": "english football season"
}' | jq

{
  "results": [
    {
      "name": "1951–52 Southern Football League",
      "abstract": "The 1951–52 Southern Football League season was the 49th in the history of the league, an English football competition. At the end of the previous season Torquay United resigned their second team from the league. No new clubs had joined the league for this season so the league consisted of 22 remaining clubs. Merthyr Tydfil were champions for the third season in a row, winning their fourth Southern League title. Five Southern League clubs applied to join the Football League at the end of the season, but none were successful.",
      "identifier": 32644089,
      "url": "https://en.wikipedia.org/wiki/1951%E2%80%9352_Southern_Football_League"
    },
    {
      "name": "1997–98 Blackburn Rovers F.C. season",
      "abstract": "During the 1997–98 English football season, Blackburn Rovers competed in the FA Premier League.",
      "identifier": 29000478,
      "url": "https://en.wikipedia.org/wiki/1997%E2%80%9398_Blackburn_Rovers_F.C._season"
    },
    {
      "name": "1993 Football League Cup final",
      "abstract": "The 1993 Football League Cup final took place on 18 April 1993 at Wembley Stadium, and was played between Arsenal and Sheffield Wednesday. Arsenal won 2–1 in normal time, in what was the first of three Wembley finals between the two sides that season; Arsenal and Wednesday also met in the FA Cup final of that year, the first time ever in English football. The match was the first match in which any European clubs had used squad numbers and player names on their shirts. On this occasion, as in the FA Cup final and replay that year, players wore individual numbers which were retained for the FA Cup finals. Coincidentally, the first occurrence of players wearing numbered shirts came on 25 August 1928, when Arsenal and Chelsea wore numbered shirts in their matches against The Wednesday and Swansea Town, respectively. Squad numbers became compulsory for Premier League clubs from August 1993. In the game, Wednesday's John Harkes scored the opener in the 8th minute, before Paul Merson equalised for Arsenal. Merson then set up Steve Morrow for the winner. In the celebrations after the match, Arsenal skipper Tony Adams attempted to pick up Morrow and parade him on his shoulders, but Adams slipped and Morrow awkwardly hit the ground. He broke his arm and had to be rushed to hospital. Unable to receive his winner's medal on the day, he was eventually presented with it before the start of the FA Cup Final the following month.",
      "identifier": 7902567,
      "url": "https://en.wikipedia.org/wiki/1993_Football_League_Cup_final"
    },
    {
      "name": "1989–90 Middlesbrough F.C. season",
      "abstract": "During the 1989–90 English football season, Middlesbrough F.C. competed in the Football League Second Division.",
      "identifier": 59075107,
      "url": "https://en.wikipedia.org/wiki/1989%E2%80%9390_Middlesbrough_F.C._season"
    }
  ]
}
```

Some tips:
- Read the inverse index you created with Spark from the file system to know which documents contain any given word.
- Use set intersections to find the document ids that contain all the query words.
- Read the files from the file system repository you created in AD1Q1 to find the abstract, uri and title for any given id. 

### [AD1Q4] Use AWS to compute the inverted index for the first 10k, 100k and 1M articles

Use AWS to compute the inverted index for the much larger datasets.
