## Question 1a

As I was not sure how to approach thi question, so I started from basics hoping I get somewhere and have an answer. Here are my thought process steps by step:

1. Bernoulli Dsitribution 
    * Parameters - p
2. Beta Distribution
    * Conjugate of Bernoulli distribtuion, so it can be used to represent the probabilities over the parameters of the Bernoulli distribution.
    * Parameter - alpha and beta all positive.
3. Estimate alpha and beta
    * There are few methods to do so, but I am taking the Maximum Likelihood approach. Turns out that one is rather complex.
    
## Question 1b

```python
'''
Using Python 3.

Question 1b) below
'''

import random
p1 = 0.923456
p2 = 0.24767
max_reward = 0
count_p1 =0
count_p2 =0
p2_reward =0
p1_reward =0
previous_p2 = 0
previous_p1 = 0
continue_iteration_p1 = True
continue_iteration_p2 = True


def tossCoin(p: float) -> int:
	return 1 if random.random() < p else 0


def flip(n: int ) -> int:
	global continue_iteration_p1, continue_iteration_p2, count_p2, count_p1, p2_reward,p1_reward, previous_p2, previous_p1, max_reward
	
	while(n>0):
		if n%2 == 0:
			if continue_iteration_p2:
				count_p2 +=1
				temp = tossCoin(p2)
				p2_reward += temp
				max_reward += temp
				if count_p2%100 ==0 and round(previous_p2,3) == round((p2_reward/count_p2),3):
						continue_iteration_p2 = False
				else:
					previous_p2 = p2_reward/count_p2

		elif continue_iteration_p1:
			count_p1 +=1
			temp = tossCoin(p1)
			p1_reward +=  temp
			max_reward += temp
			if count_p1%100 ==0 and round(previous_p1,3) == round((p1_reward/count_p1),3):
					continue_iteration_p1 = False
			else:
					previous_p2 = p2_reward/count_p2

		elif estimated_p1 >= extimated_p2:
			max_reward += tossCoin(p1)
		else:
			max_reward += tossCoin(p2)
		n-=1
	return max_reward
	
 if __name__ == '__main__':
 	main()
# assuming number of times we flip a coin is very large
# switch between p1 and p2 
# add max_reward and the ones of p1 an p2 seperately
# work out p1 and p2 iteratively - keep it simple.  p = total(heads)/total(trial for p)
# do such until p1 and p2 converge - our convergence is 3dp. estimate p every 100 trials
# once converge then stop and choose highest probability between p1 and p2 and use that for the remaining of trials

# there is an infinite loop somewhere when p1 and p2 are randomly chosen. Need to check that.
# there is certainly a better approach than the one I have stated above. But this si the first one I camme up with.
#there is also a better way to write the code above...
```

## Question 2
```sql
CREATE TABLE contact_local (
	email TEXT Primary Key,
	name VARCHAR(250),
	last_updated TIMESTAMP)

CREATE TABLE contact_remote (
	email TEXT Primary Key,
	name VARCHAR(250),
	last_updated TIMESTAMP)


--Write SQL queries to show which rows are out of sync and then update both tables so that they have the most uo-to-date results

--1. All the rows that are not in sync
select * from contact_local, contact_remote
full outer join contact_remote on contact_local.email = contact_remote.email
where contact_local.* is null 
or contact_remote.* is null 

--2. Update tables

--Add missing emails
begin transaction

update contact_local cl
	set contact_remote.email = cl.email
	from contact_remote cr
	left outer join cl
	on cr.email = cl.email
	where cr.email is null 

update contact_remote cr
	set contact_remote.email = cl.email
	from contact_local cl
	left outer join cr
	on cr.email = cl.email
	where cl.email is null 

/*Update each row according to the latest_updated timestamp*/
update contact_local.name, contact_local.last_updated
	set contact_remote.email = contact_local.email
	from contact_remote
	left outer join contact_local
	on contact_remote.email = contact_local.email
	where contact_local.last_updated < contact_remote.last_updated

update contact_remote.name, contact_remote.last_updated
	set contact_remote.email = contact_local.email
	from contact_local 
	left outer join contact_remote
	on contact_local.email = contact_remote.email
	where contact_remote.last_updated < contact_local.last_updated

if not exist (
	select * from contact_local, contact_remote
	full outer join contact_remote on contact_local.email = contact_remote.email
	where contact_local.* is null 
	or contact_remote.* is null )
	commit transaction
else rollback transaction
```

## Question 3
```scala

// Start Spark Session
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{collect_list, udf, lit}
val spark = SparkSession.builder().getOrCreate()

//step 1 Read csv file from HDFS
val df = spark.read.
option("delimiter","\t").
option("header","true").
option("inferSceme", "true").
csv("hdfs:///data/$clientID")

//Create The Cleint Data for testing
val a_2 = Seq(
  ("a", 1),
  ("a", 2),
  ("a", 3),
  ("b", 2),
  ("b", 4),
  ("c", 1),
  ("c", 3),
  ("d", 1),
  ("d", 2),
  ("d", 3),
  ("d", 4)).toDF("clientID_1", "recordID")

a_2.show()

a_2.map()
val a_3 = a_2.groupBy($"recordID").agg(concat_ws(",", collect_list($"clientID_1")).alias("clientID"))
a_3.show()


val a_6 = a_2.join(a_3_2, a_2.col("clientID") != a_3_2.col("clientID"))
a_6.show()

//val distinct_clientID = a_2.groupBy($"clientID_1").count()

//distinct_clientID.show()

```
 
