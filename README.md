Directed ReBalancing
===============

### to build
mvn package


### to run
Directed Rebalancer:

Main {original file or directory} {numberOfMappers} {lowerLimitDate} {lowerLimitTime} {upperLimitDate} {upperLimitTime} {shouldDoDeletes}

### Example of my run:
hadoop jar directedReBalancer.jar ss/input 2 2014-10-08 12:28 2014-10-08 12:34 false 