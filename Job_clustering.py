


import csv
import re
import string 
import math
import sys
import Stemmer
import operator



f = open(sys.argv[2],'a')

def job_data():
	reader = csv.DictReader(open(sys.argv[1],'rb'),delimiter = '\t')
	job_details_list = []
	for job in reader:
		job_details = {}
		job_details['JobID'] = job['JobID']
		job_details['Description'] = job['Description'].decode('utf-8')
		job_details['Requirements'] = job['Requirements'].decode('utf-8')
	
		job_details_list.append(job_details)
        return job_details_list

def document_frequency(Description,Requirements):
	
	f = open('C:\Users\sxc1653\Downloads\stop.tsv')
	stop_list = f.readlines()
	
	for j in range(0,len(stop_list)):
		stop_list[j] = stop_list[j].rstrip('\n')

	
	doc_frequency = {}
	Document = re.sub('<[^>]*>', '', Description) + re.sub('<[^>]*>', '', Requirements)
	delimiters = "-","/"," " 
	pattern = '|'.join(map(re.escape,delimiters))
	Document = re.split(pattern,Document)
	document = []
	for word in Document:
		if "&nbsp" not in word and "\\r" not in word and word not in stop_list:
			word = word.lower()
			word = word.encode('ascii', errors = 'ignore')
			stemmer = Stemmer.Stemmer('english')
			word = stemmer.stemWord(word)
			document.append(word)

        document_updated = []
	for letter_word in document:
		updated_word = ""
		for letter in letter_word:
			if letter not in string.punctuation:
				updated_word = updated_word + letter
                document_updated.append(updated_word)


	document_updated1 = []
	for letter_word1 in document:
		updated_word = ""
		for letter in letter_word:
			if letter not in string.punctuation:
				updated_word = updated_word + letter
                document_updated.append(updated_word)
	for words in document_updated:
		if doc_frequency.has_key(words):
			doc_frequency[words] = doc_frequency[words] + 1
		else:
			doc_frequency[words] = 1
	return doc_frequency


def norm(freq_list):
	temp = 0
	for frequency in freq_list:
		temp = float(temp) + (float(frequency)*float(frequency))
	norm_value = math.sqrt(temp)	
	return norm_value


def distance():
	
        
	job_details_list = job_data()

	cluster_count = 1
	doc_freq_list = []
	job_cluster = {}
	outlier_list = []
		
	for job1 in job_details_list:
		jobid1 = job1['JobID']
		if jobid1 not in job_cluster.keys():
			flag = 0
			job_cluster[jobid1] = cluster_count
			count = job_details_list.index(job1)
			doc_freq_dict1 = document_frequency(job1['Description'],job1['Requirements'])
			freq_list1 = []
			for words1,freq1 in doc_freq_dict1.items():
				freq_list1.append(freq1)
			mod_doc1 = norm(freq_list1)
			sim_list = []
			for job2 in job_details_list[count + 1:]:
				jobid2 = job2['JobID']
				if jobid2 not in job_cluster.keys():
					doc_freq_dict2 = document_frequency(job2['Description'],job2['Requirements'])
					freq_list2 = []
                                        
					for words2,freq2 in doc_freq_dict2.items():
						freq_list2.append(freq2)
					
					mod_doc2 = norm(freq_list2)
					dot_product = 0
					for k,v in doc_freq_dict1.items():
						if doc_freq_dict2.has_key(k):
							dot_product = float(dot_product) + (float(v)*float(doc_freq_dict2[k]))
					similarity = dot_product/(mod_doc1 * mod_doc2)
					
					if similarity > 0.8:
						flag = 1 
						job_cluster[jobid2] = cluster_count
						jobid = str(jobid2)
                                                cluster_num = str(cluster_count)
                                                record = jobid+'\t'+cluster_num
                                                f.write(record+'\n')
						
						print jobid2,':',job_cluster[jobid2],':',len(job_cluster)
						
					 
			
			if flag == 0:
				del job_cluster[jobid1]
				outlier_list.append(jobid1)
				cluster_count = cluster_count - 1
			else:
                                job_cluster[jobid1] = cluster_count
                                jobid = str(jobid1)
                                cluster_num = str(cluster_count)
                                record = jobid+'\t'+cluster_num
                                f.write(record+'\n')
                          
			
			cluster_count = cluster_count + 1
			
                        
	print len(job_cluster)
	print len(outlier_list)

	if len(outlier_list) > 0:
                sim_dict = {}
                for job1 in job_details_list:
                        jobid1 = job1['JobID']
                        if jobid1 in outlier_list and jobid1 not in job_cluster.keys():
                                doc_freq_dict1 = document_frequency(job1['Description'],job1['Requirements'])
                                freq_list1 = []
                                for words1,freq1 in doc_freq_dict1.items():
                                        freq_list1.append(freq1)
                                mod_doc1 = norm(freq_list1)
                                sim_dict = {}
                                for job2 in job_details_list:
                                        jobid2 = job2['JobID']
                                        doc_freq_dict2 = document_frequency(job2['Description'],job2['Requirements'])
					freq_list2 = []
                                        
                                        for words2,freq2 in doc_freq_dict2.items():
                                                freq_list2.append(freq2)
					
                                        mod_doc2 = norm(freq_list2)
                                        dot_product = 0
                                        for k,v in doc_freq_dict1.items():
                                                if doc_freq_dict2.has_key(k):
                                                        dot_product = float(dot_product) + (float(v)*float(doc_freq_dict2[k]))
                                                        similarity = dot_product/(mod_doc1 * mod_doc2)
                                                        sim_dict[jobid2] = similarity
                                sim_jobid = max(sim_dict.iteritems(), key=operator.itemgetter(1))[0]
                                if job_cluster.has_key(sim_jobid):
                                        job_cluster[jobid1] = job_cluster[sim_jobid]
                                        print jobid1,':',job_cluster[jobid1],':',len(job_cluster)
                                        jobid = str(jobid1)
                                        cluster_num = str(job_cluster[jobid1])
                                        record = jobid+'\t'+cluster_num
                                        f.write(record+'\n')
			
                                
                                
				
	
distance()
