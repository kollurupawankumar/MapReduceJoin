# MapReduce Join

1. The main functionality of the project is to join two data sets and then filter it out.
2. This project consists of two sections.
	a. Mapper
	b. Reducer
3. Unit test cases are also added along with the code. Used MRUnit for it.


# Block diagram .
![Alt text](src/main/resources/block_diagram.jpg?raw=true "Block Diagram")
	
# Mapper Functionality:
   In the mapper function, the join is performed. So that it will result into map side join and the performance will be good. in case of larger datasets. Here the main technique is in selecting the smaller dataset and place it in the cache, else it will result into performance issues.
   

# Reducer Functionality:
   In the reducer coding the filtering is done. We know that mapper output is always keys value pair. So if we are filtering the result in the reducer then it is better to keep the filtered columns as keys in the mapper phase so we can filter the groups faster, than performing the filter condition one after the other.   
   
   
# Any assumptions made along with the code.
1. Considered a seperate list of all the EU countries for filtering out. 


# Tech stack used 

1. Maven 
2. Java
3. Hadoop jars for writing the mapReduce code
4. MRUnit for unit testing the mapReduce code 


# Future
1. Now it is as per the requirement, in future i want to make it as generic as possible so that for any sort of joing and filter we can use this code/jar


# Additional code details 
Implemeted the same code with Apache Pig and placed it in the folder