import pandas as pd
import requests
import json
from utils import conn_str,SOLR_url,Core_ip,max_slot,slot_time
import gc

import numpy as np
import psycopg2
import datetime
import json


conn = psycopg2.connect(conn_str)
cursor = conn.cursor()
slot_list=[]

def next_weekday(d, weekday):
    days_ahead = weekday - d.weekday()
    if days_ahead <= 0: # Target day already happened this week
        days_ahead += 7
    return d + datetime.timedelta(days_ahead)

def api_date(list_date,start,slot):
    a_date=list_date[start]
    day = '%02d' % a_date.day
    curr_month = '%02d' % a_date.month
    curr_year = str(a_date.year)
    #format- 02/20/2019 18:30:00
    resp_date=  '%s/%s/%s %s' % (curr_month, day, curr_year,slot_time[slot])
    return resp_date



def temp(f,k,bb,reg):
    values = np.array([
        [19, 2, 3, 4, 5, 6],
        [3, 6, 6, 1, 2, 6],
        [3, 8, 5, 1, 2, 9],
        [1, 4, 2, 7, 1, 3]])
    values1 = values.sum(1)
    i = np.where(values1 == np.min(values1))

    temp=np.array(values[i,:])

    k=np.argmin(temp)



    values[i,k]+=1
    print values[i]
    values1 = values.sum(1)
    i = np.where(values1 == np.min(values1))
    print values1
    print values[i, :]
    temp = np.array(values[i, :])
    print temp
    k = np.argmin(temp)
    print k

    values[i, k] += 100
    print values[i]

    return

    k = np.where(values[i] == np.min(values[i]))
    print k
    print values[k]
    print values[i][k]
    return


    values1 = values.sum(1)
    k=np.where(values1==np.min(values1))
    print values1

    print values[k]
    return

    print values.min()
    i, j = np.where(values==values.min())
    print i,j
    print i[0],j[0]
    return


    date_list = []

    exclusion_date_list = []
    # fill above date list from postgres sql exclusion query
    d = datetime.datetime.now().date()
    next_date = next_weekday(d, 0)
    for i in range(6):
        date_n = next_date + datetime.timedelta(i)

        #print(date_n)

        if not date_n in exclusion_date_list:
            date_list.append(date_n)

    cursor = conn.cursor()
    d=date_list[1]
    #d = datetime.datetime.now().date()

    cursor.execute("UPDATE public.customer SET meeting_date=(%s) WHERE id = (%s)",
                   (d, "1",));
    print api_date(date_list,1,1)
    conn.commit()
    return
    values = np.array([
        [19, 2, 3, 4, 5, 6],
        [3, 6, 6, 1, 2, 6],
        [3, 8, 5, 1, 2, 9],
        [1, 4, 2, 7, 1, 3]])
    min_slot = values.min()
    i,j = np.where(values == min_slot)
    print i[0], j[0]
    values[i[0]][j[0]]+=1
    #ij_min = tuple([i.item() for i in ij_min])
    print values
    return


    referral_dict = {"fe": f, "bb": bb, "key": k, "reg": reg}
    min_referral = {"fe": f, "bb": bb, "key": k, "reg": reg}
    referral_dict=sorted(referral_dict, key=referral_dict.get, reverse=True)
    for ele in range(4):
     print min_referral[referral_dict[ele]]
    df = pd.read_sql('select * from tbl_data_holiday', con=conn)
    print df

    cursor = conn.cursor()
    d = datetime.datetime.now().date()
    d=d-datetime.timedelta(days=9)
    cursor.execute("UPDATE public.customer SET meeting_date=(%s) WHERE id = (%s)",
                   ( d,"1",));
    conn.commit()
    d = datetime.datetime.now().date()
    query = "select * from customer where meeting_date is NULL and activity_type= 'GTME_FE'"
    l_query = "select * from customer where meeting_date < '2019-02-22' and activity_type= 'GTME_FE'"
    #query = query % (d)

    df_referral = pd.read_sql(query, con=conn)
    df_emp=pd.read_sql(l_query, con=conn)
    frames=[df_referral,df_emp]
    f=pd.concat(frames,ignore_index=True)
    print f
    cursor.close()

def create_ref_activity(type,start_date,first_name,contact_no,dse_id,position_id,application,channel_type):

    input_data=  {
    "activity_type":type,
    "activity_status":"Open",
    "planned_start":start_date,
    "emp_row_id":"1-7Q6GVV9",
    "comments":{
        	"first_name":first_name,
            "last_name":".",
            "contact_no":contact_no,

            "dse_id":dse_id,
            "application":application,
            "channel_type":channel_type

    },



        "login_username":dse_id
    }
    activity_creation_response = requests.post(Core_ip + "api/activity/create/GTME/",
                                               data=json.dumps(input_data),
                                               headers={"Content-Type": "application/json"})
    print input_data
    return activity_creation_response.status_code


def create_activity(dse_id,position_id,contact_id,opportunity_id,start_date):


    input_data = {
        "activity_status": 'Open',
        "activity_type": 'Follow-Up',
        "planned_start": start_date,
        "comments": 'GTME UPDATE',
        "contact_id": contact_id,
        "opty_id": opportunity_id,

        "employee_id": position_id,
        "login_username": dse_id,
        "planned_start_date": start_date
    }

    #logger.info("Follow up activity creation with " + json.dumps(input_data))

    activity_creation_response = requests.post(Core_ip + "api/activity/create/",
                                               data=json.dumps(input_data),
                                               headers={"Content-Type": "application/json"})

    return activity_creation_response.status_code




def update_activity(activity_id,dse_id,emp_id,opty_id,contact_id,start_date):


    request_json={

        "activity_status": "Done",
        "activity_id":activity_id,
        "comments": "GTME UPDATE",
        "opty_id":  opty_id,
        "contact_id": contact_id,
        "activity_type": "Follow-Up",
        "login_username": dse_id,
        "employee_id": emp_id,
        "next_planned_date": start_date
    }

    activity_update_response = requests.post(Core_ip + "api/activity/update/",
                                             data=json.dumps(request_json),
                                             headers={"Content-Type": "application/json"})

    return activity_update_response.status_code

1, 3, 2, 4,3, 4, 4, 5
def slotting(dse_id,position_id,exclusion_date_list,meeting_max,c0_param,c1_param,c1A_param,c2_param,fe_priority,fe_min,bb_priority,bb_min,key_priority,key_min,reg_priority,reg_min):



    counter=0
    if not dse_id:
        return
    if not c0_param:
        return
    if not c1_param:
     return
    if not c1A_param:
     return
    if not c2_param:
     return
    print 1

    C0_query="solr/ACTIVITIES_NEW/select?fq=EVT_STAT_CD_s:OPEN%%20AND%%20TODO_CD_s:follow-up%%20AND%%20STG_NAME_s:%%22c0%%20(prospecting)%%22%%20AND%%20OWNER_PER_LOGIN_s:%%22%s%%22%%20AND%%20TODO_PLAN_END_DT_dt:[NOW-90DAY/DAY%%20TO%%20NOW-%sDAY/DAY]&indent=on&q=*:*&rows=1000&sort=TODO_PLAN_END_DT_dt%%20desc&wt=json"


    C0_query =C0_query % (dse_id,c0_param)

    C0_query=SOLR_url+C0_query

    print C0_query







    C0_resp=requests.get(C0_query)

    if C0_resp.status_code == 200:
        c0_resp = C0_resp.json()
        c0_total_count = c0_resp.get('response').get('numFound')
        print c0_total_count
        c0_resp = c0_resp.get('response').get('docs')

    #fetching C1 data
    C1_query="solr/ACTIVITIES_NEW/select?fq=EVT_STAT_CD_s:OPEN%%20AND%%20TODO_CD_s:follow-up%%20AND%%20STG_NAME_s:%%22c1%%20(quote%%20tendered)%%22%%20AND%%20OWNER_PER_LOGIN_s:%%22%s%%22%%20AND%%20TODO_PLAN_END_DT_dt:[NOW-90DAY/DAY%%20TO%%20NOW-%sDAY/DAY]&indent=on&q=*:*&rows=1000&sort=TODO_PLAN_END_DT_dt%%20desc&wt=json"
    C1_query=C1_query % (dse_id,c1_param)
    C1_query=SOLR_url+C1_query
    C1_resp=requests.get(C1_query)
    
    
        
    if C1_resp.status_code == 200:
        c1_resp = C1_resp.json()
        c1_total_count = c1_resp.get('response').get('numFound')
        print c1_total_count
        c1_resp = c1_resp.get('response').get('docs')

    # fetching C1A data
    C1A_query = "solr/ACTIVITIES_NEW/select?fq=EVT_STAT_CD_s:OPEN%%20AND%%20TODO_CD_s:follow-up%%20AND%%20STG_NAME_s:%%22C1A%%20(papers%%20submitted)%%22%%20AND%%20OWNER_PER_LOGIN_s:%%22%s%%22%%20AND%%20TODO_PLAN_END_DT_dt:[NOW-90DAY/DAY%%20TO%%20NOW-%sDAY/DAY]&indent=on&q=*:*&rows=1000&sort=TODO_PLAN_END_DT_dt%%20desc&wt=json"
    C1A_query = C1A_query % (dse_id, c1A_param)
    C1A_query = SOLR_url + C1A_query
    C1A_resp = requests.get(C1A_query)
    
    if C1A_resp.status_code == 200:
        c1A_resp = C1A_resp.json()
        c1A_total_count = c1A_resp.get('response').get('numFound')
        print c1A_total_count
        c1A_resp = c1A_resp.get('response').get('docs')

    C2_query = "solr/ACTIVITIES_NEW/select?fq=EVT_STAT_CD_s:OPEN%%20AND%%20TODO_CD_s:follow-up%%20AND%%20STG_NAME_s:%%22C2%%20(adv.%%20received)%%22%%20AND%%20OWNER_PER_LOGIN_s:%%22%s%%22%%20AND%%20TODO_PLAN_END_DT_dt:[NOW-90DAY/DAY%%20TO%%20NOW-%sDAY/DAY]&indent=on&q=*:*&rows=1000&sort=TODO_PLAN_END_DT_dt%%20desc&wt=json"
    C2_query = C2_query % (dse_id, c2_param)
    C2_query = SOLR_url + C2_query
    C2_resp = requests.get(C2_query)

    if C2_resp.status_code == 200:
        c2_resp = C2_resp.json()
        c2_total_count = c2_resp.get('response').get('numFound')
        print c2_total_count
        c2_resp = c2_resp.get('response').get('docs')


    date_list=[]


    #fill above date list from postgres sql exclusion query
    d = datetime.datetime.now().date()
    next_date = next_weekday(d, 0)
    for i in range(6):
        date_n=next_date + datetime.timedelta(i)



        if not date_n in exclusion_date_list:
            print "week day"
            print date_n
            print "done"
            date_list.append(date_n)

        else:
            print "exclusion"
            print date_n

    #print date_list

    df_c0=pd.DataFrame(c0_resp)
    df_c1 = pd.DataFrame(c1_resp)
    df_c1A = pd.DataFrame(c1A_resp)
    df_c2 = pd.DataFrame(c2_resp)

    frames=[df_c0,df_c1,df_c1A,df_c2]

    final_df=pd.concat(frames,ignore_index=True)
    #print final_df
    final_df.to_csv('out.csv', sep='\t', encoding='utf-8')



    #slotting loop for
    week_slot_counter=1
    date_list_counter=0
    slot_counter=0


    #Defining slot counter for entire date list in below matrix
    h, w = len(date_list), max_slot;
    week_alloc = [[0 for x in range(w)] for y in range(h)]
    print week_alloc

    if not final_df.empty:
        for row in final_df.itertuples(index=True, name='Pandas'):

            activity_id= getattr(row,"ROW_ID")
            if not activity_id is None:
                opty_id=getattr(row,"OPTY_ID_s")
                contact_id=getattr(row,"PR_CON_ID_s")
                #start_date=date_list[date_list_counter]
                slot=slot_counter
                start_date=api_date(date_list,date_list_counter,slot)
                upd_resp=update_activity(activity_id, dse_id, position_id, opty_id, contact_id, start_date)
                #upd_resp=200

                if upd_resp==200:
                    create_resp=create_activity(dse_id,position_id,contact_id,opty_id,start_date)

                #handle above response throw activity id in another array if failed response or use for loop with max 5 iteration for update
                    #create_resp=201

                    if create_resp==201:

                        print getattr(row,"STG_NAME_s")

                        week_alloc[date_list_counter][slot_counter]+=1

                        print week_alloc


                week_slot_counter+=1
                date_list_counter+=1
                if week_slot_counter >len(date_list):
                    week_slot_counter=1
                    date_list_counter=0
                    slot_counter+=1
                    if slot_counter>=max_slot:
                        slot_counter=0


            if date_list_counter >=len(date_list):
                week_slot_counter=1
                date_list_counter=0
                slot_counter=0


    #Referral allocation



    week_alloc=np.array(week_alloc)
    print "allocation status after pipeline"
    print week_alloc
    priority_dict={"GTME_FE":fe_priority,"GTME_BB":bb_priority,"GTME_KC":key_priority,"GTME_RV":reg_priority}
    referral_dict={"GTME_FE":fe_priority,"GTME_BB":bb_priority,"GTME_KC":key_priority,"GTME_RV":reg_priority}
    min_referral={"GTME_FE":fe_min,"GTME_BB":bb_min,"GTME_KC":key_min,"GTME_RV":reg_min}
    referral_dict=sorted(referral_dict, key=referral_dict.get, reverse=True)

    #for minimum allocation meeting date null or less than timezone.now will be considered
    date_today= datetime.datetime.now().date()
    for ele in range(4):
        null_query = "select * from customer where meeting_date is NULL and activity_type='%s' and dse_id='%s'"
        val_query = "select * from customer where meeting_date < '%s' and activity_type='%s' and dse_id='%s'"

        null_query=null_query % (referral_dict[ele],dse_id)
        val_query=val_query % (date_today,referral_dict[ele],dse_id)
        df_null=pd.read_sql(null_query,con=conn)
        df_val= pd.read_sql(val_query, con=conn)
        frames=[df_null,df_val]
        df_referral=pd.concat(frames,ignore_index=True)
        if not df_referral.empty :


            m_ref=min_referral[referral_dict[ele]]
            for row in df_referral.itertuples(index=True, name='Pandas'):
                if m_ref is not 0:
                    min_slot=week_alloc.min()
                    i,j = np.where(week_alloc == min_slot)
                    start_date=api_date(date_list,i[0],j[0])
                    first_name=getattr(row, "customer_name")
                    contact_no=getattr(row, "contact_num")
                    application=getattr(row,"application")
                    channel_type=getattr(row,"channel_type")
                    ref_resp=create_ref_activity(referral_dict[ele], start_date, first_name, contact_no, dse_id,position_id, application,channel_type)
                    #ref_resp=201
                    if ref_resp==201:
                        week_alloc[i[0]][j[0]]+=1

                        d=date_list[i[0]]

                        cursor.execute("UPDATE public.customer SET meeting_date=(%s) WHERE id = %s",
                                   (date_today, getattr(row,"id"),));
                        conn.commit()
                    m_ref-=1







        df_null.iloc[0:0]
        df_val.iloc[0:0]

    # going for normal scheduling if there is any
    #Taking normal sum for each column and

    print "week alloc after min alloc"
    print week_alloc
    print "week alloc sum"
    print week_alloc.sum(1)

    print "meeting_max"
    print meeting_max
    if (meeting_max*len(date_list)) <= week_alloc.sum():
        print "week_alloc_sum"
        print week_alloc.sum()
        return
    else:
        ref_sum = 0

        diff=(meeting_max*len(date_list))-week_alloc.sum()


        for ele in priority_dict:
            print ele

            ref_sum = ref_sum +(min_referral[ele]*priority_dict[ele])
        if ref_sum==0:
            return

        for ele in min_referral:
            null_query = "select * from customer where meeting_date is NULL and activity_type='%s' and dse_id='%s'"
            val_query = "select * from customer where meeting_date < '%s' and activity_type='%s' and dse_id='%s'"
            null_query = null_query % (ele,dse_id)
            val_query = val_query % (date_today, ele,dse_id)
            df_null = pd.read_sql(null_query, con=conn)
            df_val = pd.read_sql(val_query, con=conn)
            frames = [df_null, df_val]
            df_referral = pd.concat(frames, ignore_index=True)
            if not df_referral.empty:
                slot_iteration=0


                slot_iteration=(min_referral[ele]*priority_dict[ele]*diff)/ref_sum
                print ele
                print slot_iteration
                min_slot = week_alloc.min()


                """i, j = np.where(week_alloc == min_slot)
                print i,j
                values1 = week_alloc.sum(1)
                i = np.where(values1 == np.min(values1))
                print values1
                j=np.where(week_alloc(i)==np.min(week_alloc(i)))
                print values[k]"""

                #In below 4 lines for remaining allocation find minimum val and allocate sum in that val i is Date val and j is slot val
                week_alloc_sum = week_alloc.sum(1)
                i=np.argmin(week_alloc_sum)
                #i = np.where(week_alloc_sum == np.min(week_alloc_sum))

                temp = np.array(week_alloc[i, :])

                j = np.argmin(temp)

                print " i & j is "
                print i,j






                for row in df_referral.itertuples(index=True, name='Pandas'):
                    if slot_iteration > 0:


                        """if k>=len(i):
                            min_slot = week_alloc.min()
                            i, j = np.where(week_alloc == min_slot)
                            k = 0"""

                            #print week_alloc.argmin(axis=1)

                        #i,j=week_alloc.argmin(axis=1)

                        #i, j = np.where(week_alloc == min_slot)

                        start_date = api_date(date_list, i, j)
                        first_name = getattr(row, "customer_name")
                        contact_no = getattr(row, "contact_num")
                        application = getattr(row, "application")
                        channel_type = getattr(row, "channel_type")
                        ref_resp = create_ref_activity(ele, start_date, first_name, contact_no, dse_id,position_id,
                                                       application, channel_type)

                        #ref_resp=201
                        if ref_resp == 201:



                            week_alloc[i,j] += 1
                            print "week alloc counter incre of ref"

                            print week_alloc
                            slot_iteration -= 1

                            d = date_list[i]
                            print "date for k is "
                            print d

                            cursor.execute("UPDATE public.customer SET meeting_date=(%s) WHERE id = (%s)",
                                           (d, getattr(row, "id"),));

                            conn.commit()
                            week_alloc_sum = week_alloc.sum(1)
                            i = np.argmin(week_alloc_sum)
                            # i = np.where(week_alloc_sum == np.min(week_alloc_sum))

                            temp = np.array(week_alloc[i, :])

                            j = np.argmin(temp)

                            print " i & j is "
                            print i, j

                        else:
                            break


    print "final week alloc"
    print week_alloc
    print week_alloc.sum(1)

    return













if __name__ == '__main__':
    cursor = conn.cursor()

    # d = datetime.datetime.now().date()

    portal_df = pd.read_sql("select * from tbl_gtme where allocation_status='N'", con=conn)
    """for row in portal_df.itertuples(index=True, name='Pandas'):
        id = getattr(row, "id")

        exclusion_date=[]
        exclusion_query=""""""SELECT exclusion_date FROM tbl_data_holiday INNER JOIN tbl_holiday ON 
(tbl_holiday.id=tbl_data_holiday.holiday_id) 
WHERE tbl_data_holiday.data_id='%s'"""
    """exclusion_query=exclusion_query % (id)

        print exclusion_query

        cursor.execute(exclusion_query)

        exclusion_result= cursor.fetchall()



        print (exclusion_result)"""



    #temp(4,1,2,3)
    position_id='1-7Q6GVV9'#1-7q6gvv
    exclusion_result=[]

    slotting('UY1_1000870', position_id, exclusion_result,20, 10, 7, 4, 2, 1, 0, 2, 4,3, 4, 4, 5)


    gc.collect()