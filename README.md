# Big-Data-Map-Reduce-Twitter-Recommendation-

//python function to make a list of friend and who a person follows 
def fol1(x):
	mylist=[]
	l1t=("follows",x[1])
	l1=(x[0],l1t)
	mylist.append(l1)
	return mylist


//python function to make a list of friends that follow a person
def fol2(x):
	mylist=[]
	listfollowers=list(x[1].split(","))
	for i in listfollowers:
		l2t=("followed_by",x[0])
		l2=(i,l2t)
		mylist.append(l2)
	return mylist


//python function to find friend of friends	
def fol9(x):
	mylist=[]
	l=len(x[1])
	if l==4:
		t1=x[1][1]
		t2=x[1][3]
		listofwhotheyfollow=list(t2.split(","))
		listOfPeopleThatFollowThem=list(t1.split(","))
		for followed in listofwhotheyfollow:
			for follower in listOfPeopleThatFollowThem:
				if follower not in listofwhotheyfollow:
					l1=(follower,followed)
					mylist.append(l1)
		return mylist
	return 0


//python function to add unique values to the list	
def fol4(x):
	l1=list(set(list(x[1].split(","))))
	return (str(x[0]),str(l1[:3]))



rdd=sc.textFile("/home/ec2-user/inp/input")

rdd2=rdd.map(lambda x:x.split())

rdd3=rdd2.map(lambda x:(x[0],x[1]))  

rdd4=rdd3.reduceByKey(lambda x,y:x+','+y)  

rdd5=rdd4.map(lambda x:fol1(x)).flatMap(lambda x:x)    //follows list

rdd6=rdd4.map(lambda x:fol2(x)).flatMap(lambda x:x)	  // followed by rdd

rdd7=rdd6.reduceByKey(lambda x,y:(x[0],x[1]+','+y[1])) //followed by list

rdd9=rdd7.union(rdd5) 					  //combining the two rdds	

//rdd10 is simple reduce to get tuple in the form 
(key(person),(‘follows’,[list of follows],’followed_by’,’[followed by list]’))

rdd10=rdd9.reduceByKey(lambda x,y:x+y)

rdd12=rdd10.map(lambda x:fol9(x))
//rdd to filter 0 i.e.tuples for which follows and followedby lists are not there
rdd13=rdd12.filter(lambda x:x!=0).flatMap(lambda x:x).reduceByKey(lambda x,y:x+','+y)

//rdd to filter recommendations for given users
rdd14=rdd13.filter(lambda x:x[0]=='1' or x[0]=='27' or x[0]=='31' or x[0]=='137' or x[0]=='3113')

//to get desired output
rdd15=rdd14.map(lambda x:fol4(x)).map(lambda x:(x[0]+"-should_follow->"+x[1]))

rdd15.collect()
