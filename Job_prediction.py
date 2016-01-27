import csv
import sys
import re
import datetime




t_part = datetime.datetime(2012,04,9,0,0,0,0)
csv.field_size_limit(sys.maxsize)


def input_files():
	
	#reader0 = csv.DictReader(open("/home/tej/Documents/Sem 1/Data Mining/Projects/CSE5334_Project_2/data/users.tsv",'rb'),delimiter = '\t')
	reader0 = csv.DictReader(open(sys.argv[1],'rb'),delimiter = '\t')
	#reader1 = csv.DictReader(open("/home/tej/Documents/Sem 1/Data Mining/Projects/CSE5334_Project_2/data/apps.tsv",'rb'),delimiter = '\t')
	reader1 = csv.DictReader(open(sys.argv[2],'rb'),delimiter = '\t')
	#reader2 = csv.DictReader(open("/home/tej/Documents/Sem 1/Data Mining/Projects/CSE5334_Project_2/data/jobs.tsv",'rb'),delimiter = '\t')
	reader2 = csv.DictReader(open(sys.argv[3],'rb'),delimiter = '\t')
	#reader3 = csv.DictReader(open("/home/tej/Documents/Sem 1/Data Mining/Projects/CSE5334_Project_2/data/user_history.tsv",'rb'),delimiter = '\t')
	reader3 = csv.DictReader(open(sys.argv[4],'rb'),delimiter = '\t')

	readers = [reader0,reader1,reader2,reader3]
	
	return readers 
	
	
def func():
	reader = csv.DictReader(open("/home/tej/Documents/Sem 1/Data Mining/Projects/CSE5334_Project_2/data/users.tsv",'rb'),delimiter = '\t')
	reader1 = csv.DictReader(open("/home/tej/Documents/Sem 1/Data Mining/Projects/CSE5334_Project_2/data/jobs.tsv",'rb'),delimiter = '\t')
	reader2 = csv.DictReader(open("/home/tej/Documents/Sem 1/Data Mining/Projects/CSE5334_Project_2/data/user_history.tsv",'rb'),delimiter = '\t')


	for user_history in reader2:
		if user_history['UserID'] == '520528' and user_history['Sequence'] is not None and user_history['Sequence'] == '1':
			job_title = user_history['JobTitle']

	for user in reader:
		if user['UserID'] == '520528' and user['Major'] is not None:
			major = re.compile(user['Major'], re.IGNORECASE)

	for job in reader1:
		if job_title in job['Title'] and major.search(job['Description']) is not None:
			print job['JobID'],":",job['Title'],":",job['Country'],":",job['State']

		
def majors():
	reader = csv.DictReader(open("/home/tej/Documents/Sem 1/Data Mining/Projects/CSE5334_Project_2/data/users.tsv",'rb'),delimiter = '\t')
	major_list = []
	for user in reader:
		if user['Major'] not in major_list:
			major_list.append(user['Major'])
	print major_list
		
#majors()

def job_id_u2t2():	
	
	
	#reader = csv.DictReader(open("/home/tej/Documents/Sem 1/Data Mining/Projects/CSE5334_Project_2/data/jobs.tsv",'rb'),delimiter = '\t')
	readers = input_files()
	csv.field_size_limit(sys.maxsize)
	j2_d2 = {}
	
	for line in readers[2]:
		time = line['EndDate']
		if time is not None:
			j2 = datetime.datetime(int(time[:4]),int(time[5:7]),int(time[8:10]),int(time[11:13]),int(time[14:16]),int(time[17:19])) 
		
		if j2 > t_part:
			j2_d2[line['JobID']] = line['Title']
			
	return j2_d2




def demo():
	
	jobs = ['667899']
	reader = csv.DictReader(open("/home/tej/Documents/Sem 1/Data Mining/Projects/CSE5334_Project_2/data/jobs.tsv",'rb'),delimiter = '\t')
	job_list = []
	for job in reader:
		if job['JobID'] in jobs:
			job_dict = {}
			job_dict['JobID'] = job['JobID']
			job_dict['Title'] = job['Title']
			job_dict['Country'] = job['Country']
			job_dict['State'] = job['State']
			job_dict['State'] = job['City']
	
			job_list.append(job_dict)
	print job_list
	
#demo()	

#===========================================================================================================================================#

def users2():
	#with open("/home/tej/Documents/Sem 1/Data Mining/Projects/CSE5334_Project_2/data/user2.tsv") as f:
	with open(sys.argv[5]) as f:
		users2_main = f.readlines()

	for j in range(0,len(users2_main)):
		users2_main[j] = users2_main[j].rstrip('\n')

	return users2_main


def u1_t2_profile():
	u2 = users2()
	reader5 = csv.DictReader(open("/home/tej/Documents/Sem 1/Data Mining/Projects/CSE5334_Project_2/data/users.tsv",'rb'),delimiter = '\t')
	u1_t2 = []
	
	for users in reader5:
		if users['UserID'] not in u2:
			
			u1_t2_dict = {}
			u1_t2_dict['UserID'] = users['UserID']
			u1_t2_dict['Major'] = users['Major']
			u1_t2_dict['Country'] = users['Country']
			u1_t2_dict['State'] = users['State']
			u1_t2_dict['City'] = users['City']
			
			u1_t2.append(u1_t2_dict)
	 
	print "PHASE 1 : u1_t2 profile with location and major details updated!!!"
	return u1_t2



def u1_Curr_Job():
	
	u2 = users2()
	u1_t2_list = u1_t2_profile()
	
	
	reader1 = csv.DictReader(open("/home/tej/Documents/Sem 1/Data Mining/Projects/CSE5334_Project_2/data/user_history.tsv",'rb'),delimiter = '\t')
	
	u1_CurrJob_dict = {}
	for history in reader1:
		if history['UserID'] not in u2 and history['UserID'] != '' and history['Sequence'] == '1':
			
			u1_CurrJob_dict[history['UserID']] = history['JobTitle']
	
	print "Current Jobs for U1 collected"		
	
	for u1_t2 in u1_t2_list:
		if u1_CurrJob_dict.has_key(u1_t2['UserID']):
			u1_t2['CurrJobTitle'] = u1_CurrJob_dict[u1_t2['UserID']] 
	
	
	print "PHASE 2 : U1 list updated with current job!!!"
	return u1_t2_list

#u1_Curr_Job()

def u1_joblist_t2(): 

	u1_j2 = {}
	j2 = job_id_u2t2()
	u2 = users2()
	u1_t2_list = u1_Curr_Job()
	
	reader2 = csv.DictReader(open("/home/tej/Documents/Sem 1/Data Mining/Projects/CSE5334_Project_2/data/apps.tsv",'rb'),delimiter = '\t')
	
	for app in reader2:
		if app['UserID'] not in u2 and j2.has_key(app['JobID']) and u1_j2.has_key(app['UserID']):
			u1_j2[app['UserID']].append(app['JobID'])
		else:
			u1_j2[app['UserID']] = []
			u1_j2[app['UserID']].append(app['JobID']) 
	
	print "Job list for U1 in T2 collected."
	
	for u1_t2 in u1_t2_list:
		if u1_j2.has_key(u1_t2['UserID']):
			u1_t2['JobList'] = u1_j2[u1_t2['UserID']] 
	
	print "PHASE 3 : U1 list updated with job list !!!"
	return u1_t2_list
#u1_joblist_t2()



def user2_profile():
	user2 = users2()
	readers = input_files()
	#reader3 = csv.DictReader(open("/home/tej/Documents/Sem 1/Data Mining/Projects/CSE5334_Project_2/data/users.tsv",'rb'),delimiter = '\t')
	#reader4 = csv.DictReader(open("/home/tej/Documents/Sem 1/Data Mining/Projects/CSE5334_Project_2/data/user_history.tsv",'rb'),delimiter = '\t')
	
	job_history = {}
	
	for user_history in readers[3]:
		
		if job_history.has_key(user_history['UserID']):
			job_history[user_history['UserID']].append(user_history['JobTitle'])
		
		else:
			job_history[user_history['UserID']] = []
			job_history[user_history['UserID']].append(user_history['JobTitle'])	
		
			
		
	user2_profile_list = []
	for user in readers[0]:
		if user['UserID'] in user2:
			
			user2_profile_dict = {}
			user2_profile_dict['UserID'] = user['UserID']
			user2_profile_dict['Country'] = user['Country']
			user2_profile_dict['State'] = user['State']
			user2_profile_dict['City'] = user['City']
			user2_profile_dict['Major'] = user['Major']
			if job_history.has_key(user['UserID']):
				user2_profile_dict['Job_history'] = job_history[user['UserID']]
						
			user2_profile_list.append(user2_profile_dict)
			
	print "PHASE 1 : u2 updated"		
	return user2_profile_list
	
#user2_profile()


def u1_u2_CurrJobs():
	#u1 = Thread(target = u1_CurrJob_joblist).start()
	#u2 = Thread(target = user2_profile).start()
	#u1.join()
	#u2.join()
	
	#print "Done"
	u1 = u1_joblist_t2()
	u2 = user2_profile()
	

	
	print "U1 : ",len(u1)
	print "U2 : ",len(u2)
	
	
	print "Counting U1/U2 match with same major and current job positions ..."
	u2_job_weight_list = []
	for user2 in u2:
		if user2['Major'] != 'Not Applicable' or user2['Major'] != '':
			u2_job_weight = {}
			for user1 in u1:
				if user1['Major'] != 'Not Applicable' or user2['Major'] != '' and user1.has_key('JobList'):
					if u2_job_weight.has_key(user2['UserID']) and user2.has_key('CurrJobTitle') and user1.has_key('CurrJobTitle') and user2['CurrJobTitle'] != '' and user1['CurrJobTitle'] != '' and user2['CurrJobTitle'] == user1['CurrJobTitle'] and user2['Major'] == user1['Major'] and user2['Country'] == user1['Country'] and user2['State'] == user1['State'] and user2['City'] == user1['City'] and user1.has_key('JobList'):
						u1_job_list = user1['JobList']
						job_weight = {}
						for job in u1_job_list:
							job_weight[job] = 100
							
						u2_job_weight['JobIDs'].update(job_weight)
					
						
					elif user2.has_key('CurrJobTitle') and user1.has_key('CurrJobTitle') and user2['CurrJobTitle'] != '' and user1['CurrJobTitle'] != '' and user2['CurrJobTitle'] == user1['CurrJobTitle'] and user2['Major'] == user1['Major'] and user2['Country'] == user1['Country'] and user2['State'] == user1['State'] and user2['City'] == user1['City'] and user1.has_key('JobList'):
						u1_job_list = user1['JobList']
						job_weight = {}
						for job in u1_job_list:
							job_weight[job] = 100
					
						u2_job_weight['UserID'] = user2['UserID']
						u2_job_weight['JobIDs'] = job_weight
				
					elif u2_job_weight.has_key(user2['UserID']) and user2.has_key('CurrJobTitle') and user1.has_key('CurrJobTitle') and user2['CurrJobTitle'] != '' and user1['CurrJobTitle'] != '' and user2['CurrJobTitle'] == user1['CurrJobTitle'] and user2['Major'] == user1['Major'] and user2['Country'] == user1['Country'] and user2['State'] == user1['State'] and user1.has_key('JobList'):
						u1_job_list = user1['JobList']
						job_weight = {}
						for job in u1_job_list:
							job_weight[job] = 95
							
						u2_job_weight['JobIDs'].update(job_weight)
				
					elif user2.has_key('CurrJobTitle') and user1.has_key('CurrJobTitle') and user2['CurrJobTitle'] != '' and user1['CurrJobTitle'] != '' and user2['CurrJobTitle'] == user1['CurrJobTitle'] and user2['Major'] == user1['Major'] and user2['Country'] == user1['Country'] and user2['State'] == user1['State'] and user1.has_key('JobList'):
						u1_job_list = user1['JobList']
						job_weight = {}
						for job in u1_job_list:
							job_weight[job] = 95
					
						u2_job_weight['UserID'] = user2['UserID']
						u2_job_weight['JobIDs'] = job_weight
					
					elif u2_job_weight.has_key(user2['UserID']) and user2.has_key('CurrJobTitle') and user1.has_key('CurrJobTitle') and user2['CurrJobTitle'] != '' and user1['CurrJobTitle'] != '' and user2['CurrJobTitle'] == user1['CurrJobTitle'] and user2['Major'] == user1['Major'] and user2['Country'] == user1['Country'] and user1.has_key('JobList'):
						u1_job_list = user1['JobList']
						job_weight = {}
						for job in u1_job_list:
							job_weight[job] = 90
						
						u2_job_weight['JobIDs'].update(job_weight)
				
					elif user2.has_key('CurrJobTitle') and user1.has_key('CurrJobTitle') and user2['CurrJobTitle'] != '' and user1['CurrJobTitle'] != '' and user2['CurrJobTitle'] == user1['CurrJobTitle'] and user2['Major'] == user1['Major'] and user2['Country'] == user1['Country'] and user1.has_key('JobList'):
						u1_job_list = user1['JobList']
						job_weight = {}
						for job in u1_job_list:
							job_weight[job] = 90
					
						u2_job_weight['UserID'] = user2['UserID']
						u2_job_weight['JobIDs'] = job_weight
					
					elif u2_job_weight.has_key(user2['UserID']) and user2.has_key('CurrJobTitle') and user1.has_key('CurrJobTitle') and user2['CurrJobTitle'] != '' and user1['CurrJobTitle'] != '' and user2['CurrJobTitle'] == user1['CurrJobTitle'] and user2['Country'] == user1['Country'] and user1.has_key('JobList'):
						u1_job_list = user1['JobList']
						job_weight = {}
						for job in u1_job_list:
							job_weight[job] = 85
						
						u2_job_weight['JobIDs'].update(job_weight)

					
					
					elif user2.has_key('CurrJobTitle') and user1.has_key('CurrJobTitle') and user2['CurrJobTitle'] != '' and user1['CurrJobTitle'] != '' and user2['CurrJobTitle'] == user1['CurrJobTitle'] and user2['Country'] == user1['Country'] and user1.has_key('JobList'):
						u1_job_list = user1['JobList']
						job_weight = {}
						for job in u1_job_list:
							job_weight[job] = 85
					
						u2_job_weight['UserID'] = user2['UserID']
						u2_job_weight['JobIDs'] = job_weight
					
										
			u2_job_weight_list.append(u2_job_weight)
	print u2_job_weight_list
		
	"""	
		elif user2['Major'] == 'Not Applicable' or user2['Major'] == '':
			for user1 in u1:
				if user2.has_key('CurrJobTitle') and user2['CurrJobTitle'] != '' and user1['CurrJobTitle'] != '' and user2['CurrJobTitle'] == user1['CurrJobTitle'] and user1.has_key('JobList') and user2.has_key('JobList'):
					user2['JobList'] = user2['JobList'] + user1['JobList']
				elif user2.has_key('CurrJobTitle') and user2['CurrJobTitle'] != '' and user1['CurrJobTitle'] != '' and user2['CurrJobTitle'] == user1['CurrJobTitle'] and user1.has_key('JobList'):
					user2['JobList'] = user1['JobList']
	#print u2
	
	"""


#u1_u2_CurrJobs()
	
def job_profiles():
	#reader = csv.DictReader(open("/home/tej/Documents/Sem 1/Data Mining/Projects/CSE5334_Project_2/data/jobs.tsv",'rb'),delimiter = '\t')
	
	readers = input_files()
	j2 = job_id_u2t2()
	job_profile_list = []
	
	for job in readers[2]:
		if j2.has_key(job['JobID']):
			job_profile_dict = {}
			job_profile_dict['JobID'] = job['JobID']
			job_profile_dict['JobTitle'] = job['Title']
			#job_profile_dict['Description'] = job['Description']
			#job_profile_dict['Requirements'] = job['Requirements']
			job_profile_dict['Country'] = job['Country']
			job_profile_dict['State'] = job['State']
			job_profile_dict['City'] = job['City']
			
			job_profile_list.append(job_profile_dict)
	print "PHASE 2 : Jobs profile updated"		
	return job_profile_list
	
def job_profiles_alternative():
	reader = csv.DictReader(open("/home/tej/Documents/Sem 1/Data Mining/Projects/CSE5334_Project_2/data/jobs.tsv",'rb'),delimiter = '\t')
	j2 = job_id_u2t2()
	
	job_profile_list = []
	
	job_location = {}
	
	for job in reader:
		if j2.has_key(job['JobID']):
									
			if job_location.has_key(job['Country']+job['State']):
				job_jobtitle = {}
				job_jobtitle['JobID'] = job['JobID']
				job_jobtitle['Title'] = job['Title']
				
				job_location[job['Country']+job['State']].update(job_jobtitle)
				
			elif job['Country'] != '' and job['State'] != '':
				job_jobtitle = {}
				job_jobtitle['JobID'] = job['JobID']
				job_jobtitle['Title'] = job['Title']
				
				job_location[job['Country']+job['State']] = job_jobtitle
	
			elif job_location.has_key(job['Country']):
				job_jobtitle = {}
				job_jobtitle['JobID'] = job['JobID']
				job_jobtitle['Title'] = job['Title']
				
				job_location[job['Country']].update(job_jobtitle)
				
			elif job['Country'] != '':
				job_jobtitle = {}
				job_jobtitle['JobID'] = job['JobID']
				job_jobtitle['Title'] = job['Title']
				
				job_location[job['Country']] = job_jobtitle
				
				
	
	print job_location 	

#job_profiles_alternative()
#job_profiles()
			
def u2j2():
	
	user_profile_list = user2_profile()
	j2_list = job_profiles()
	
	print len(user_profile_list)
	print len(j2_list)
	#count = 1
	for user in user_profile_list:
		if user.has_key('Job_history') and user['Job_history'] is not None:
			temp_job_list = []			
			for job in j2_list:
				if job['Country'] == user['Country'] and job['State'] == user['State']:
					temp_job = {}
					temp_job['JobID'] = job['JobID']
					temp_job['Title'] = job['JobTitle']
					temp_job['City'] = job['City']
					
					temp_job_list.append(temp_job)
			#print temp_job_list		
			job_weight = {}
			weight = [i for i in range(0,100) if i%5 == 0]
			count = 0
			for pre_job in user['Job_history']:
				i = weight[count]
				for jobs in temp_job_list:
					#user_job_dict = {}
					if user.has_key('Job_List') and pre_job != '' and pre_job in jobs['Title'] and user['City'] == jobs['City']:
						job_weight[jobs['JobID']] = 100 - i
					
						user['Job_List'].update(job_weight)
				
					elif pre_job != '' and pre_job in jobs['Title'] and user['City'] == jobs['City']:
						job_weight[jobs['JobID']] = 100 - i
						user['Job_List'] = job_weight
				
				
					elif user.has_key('Job_List') and pre_job != '' and pre_job in jobs['Title']:
						job_weight[jobs['JobID']] = 95 - i
						user['Job_List'].update(job_weight)
				
					elif pre_job != '' and pre_job in jobs['Title']:
						job_weight[jobs['JobID']] = 95 - i
						user['Job_List'] = job_weight
				
				count = count + 1
			
			"""		
			elif user.has_key('Job_List') and user.has_key('CurrJobTitle') and user['CurrJobTitle'] in job['JobTitle'] and user['Country'] == job['Country']:
				job_weight[job['JobID']] = 90
				user['Job_List'].update(job_weight)
				
			elif user.has_key('CurrJobTitle') and user['CurrJobTitle'] in job['JobTitle'] and user['Country'] == job['Country']:
				job_weight[job['JobID']] = 90
				user['Job_List'] = job_weight
			"""	 
	print "Final user2 profile with suggested jobs prepared!!!"
	return user_profile_list		
	
#u2j2()


def top_150():
	
	user2_final_list = u2j2()
	
	print "Printing top 150 suggestions..."
	c = 0
	for user2 in user2_final_list:
		user_count = 0
		if user2.has_key('Job_List'):
			for job_id, weight in user2['Job_List'].items():
				if weight == 100 or weight == 95:
					if user_count < 5:
						if c < 151:
							print user2['UserID'], '\t',job_id
							c = c + 1
						else: exit
				user_count = user_count + 1
top_150()
