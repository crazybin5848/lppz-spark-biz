use omsext;
select ds,count(orderid) from omsextorders where isnotnull(orderid) group by ds order by ds asc; 
select ds,count(1) from omsextorderlines where isnotnull(id) group by ds order by ds asc;
 
select ds,count(1) from omsextshipments where isnotnull(id) group by ds order by ds asc; 
select ds,count(1) from omsextorderlinequantities where isnotnull(id) group by ds order by ds asc; 

select ds,count(1) from omsextpaymentinfo where isnotnull(id) group by ds order by ds asc; 
select ds,count(1) from omsextbusipromotioninfo where isnotnull(id) group by ds order by ds asc; 

select ds,count(1) from omsextbusiorderlinepromotioninfo where isnotnull(id) group by ds order by ds asc; 
select ds,count(1) from omsextorderlineattributes where isnotnull(id) group by ds order by ds asc; 
select ds,count(1) from omsextorderlinedatalocationroles where isnotnull(srcid) group by ds order by ds asc; 

select ds,count(1) from omsextbusilpdeliveryedata where isnotnull(id) group by ds order by ds asc; 
select ds,count(1) from omsextbusilpdeliveryelinedata where isnotnull(id) group by ds order by ds asc; 

select ds,count(1) from omsextbusiomsinterfacemutualdata where isnotnull(id) group by ds order by ds asc; 
select ds,count(1) from omsedbbusiomsinterfacemutualwmsdata where isnotnull(id) group by ds order by ds asc; 

select ds,count(1) from omsextorderdatasrlocationids where isnotnull(srcid) group by ds order by ds asc; 

select ds,count(1) from omsextbusimergeorderpooldata where isnotnull(id) group by ds order by ds asc; 

select ds,count(1) from omsedbbusilackorder where isnotnull(id) group by ds order by ds asc; 

select ds,count(1) from omsextreturns where isnotnull(id) group by ds order by ds asc; 
select ds,count(1) from omsextreturnorderlines where isnotnull(id) group by ds order by ds asc; 

select ds,count(1) from omsextbusireturnpackagedata where isnotnull(id) group by ds order by ds asc; 
select ds,count(1) from omsextbusirefundonlydata where isnotnull(id) group by ds order by ds asc; 

select ds,count(1) from omsextbusireturnpickorder where isnotnull(id) group by ds order by ds asc; 
select ds,count(1) from omsextbusireturnpickorderline where isnotnull(id) group by ds order by ds asc; 

