# Kafka Streams Processing
## Overview

Repository contains a Spring Boot application that leverages Kafka Streams for data processing.
The application is divided into five main tasks, each addressing specific Kafka Streams scenarios.

### Task 1: Text Data Transfer
Objective: Push text data to task1-1 topic and expect to get the same data in the task1-2 topic.
### Task 2: Sentence Processing
Objective: Push sentence data to task2, split sentences into words, categorize into short and long words, filter by non-null values, 
create new messages based on length, and filter long and short messages based on the presence of the letter "a".
### Task 3: Inner Join of Two Streams
Objective: Read data from task3-1 and task3-2, filter non-null and colon-containing values, assign a new key based on the numeric part
of the value, print messages, and perform an inner join with a 30-second tolerance.
### Task 4: Custom SerDe for JSON Data
Objective: Implement a custom Serde to handle JSON messages in the task4 topic with a specified structure. Filter out null values and print the processed messages.
### Task 5: Unit Tests for Task 2 and Task 4
Objective: Write unit tests for the implemented functionality in Task 2 and Task 4.
