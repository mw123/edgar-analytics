import argparse
from datetime import datetime, timedelta

# separate data fields
def parse_request(line, field_idx):
    new_req = {}
    for field, idx in field_idx.items():
        new_req[field] = line[idx]
    return new_req

# create a new session
def create_user_session(active_sessions, new_req, line_num):
    session = {'first_req_time': new_req['date']+' '+new_req['time'],
                'last_req_time': new_req['date']+' '+new_req['time']}
    session['duration'] = 1
    session['page_count'] = 1
    session['line_num'] = line_num
    active_sessions[new_req['ip']] = session 
 
# update web document request count for given user session
def update_session_req(active_sessions, req, elapsed_time):
    active_sessions[req['ip']]['page_count'] += 1
    active_sessions[req['ip']]['duration'] += elapsed_time
    active_sessions[req['ip']]['last_req_time'] = req['date']+' '+req['time']

# calculate time difference between curr_time and new_req time in seconds 
def time_diff_sec(new_time, curr_time, time_fmt):
    new_time = datetime.strptime(new_time, time_fmt)
    if curr_time is None:
        curr_time = new_time
    else:
        curr_time = datetime.strptime(curr_time, time_fmt)
    
    return int(timedelta.total_seconds(new_time-curr_time))

# log sessions that end in simutaneous time step, ordered first by first request time, then 
# by input line number
def log_session(out_file, active_sessions, ended_sessions, time_fmt):
    ended_sessions_sorted = sorted(ended_sessions.items(), 
                                    key=lambda x: (datetime.strptime(x[1]['first_req_time'], time_fmt).date(), x[1]['line_num']))
    for sess, _ in ended_sessions_sorted:
        output = sess+','+active_sessions[sess]['first_req_time']+','+ \
                    active_sessions[sess]['last_req_time']+','+ \
                    str(active_sessions[sess]['duration'])+','+str(active_sessions[sess]['page_count'])+'\n'
        out_file.write(output)
        del active_sessions[sess]


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='EDGAR request pipeline')
    parser.add_argument("in_csv", action='store', help='Specify path to input csv file relative to re.')
    parser.add_argument("in_inactive_period",  action='store', help='Specify full path to file containing inactivity period.')
    parser.add_argument("--output",  action='store', type=str, default="./output/sessionization.txt", help='Full path to where the output of this program is stored.')
     
    args = parser.parse_args()

    # read inactive period from input file
    with open(args.in_inactive_period) as in_file:
        max_inactive_period = int(in_file.readline())
    
    active_sessions = {} # each active session is uniquely identified by user ip address
    data_field_idx = {} # stores index position of every field name in each web request
    time_fmt = '%Y-%m-%d %H:%M:%S'
    curr_time = None # current time as '%Y-%m-%d %H:%M:%S'

    with open(args.output, 'w+') as out_file:
        # begin reading weblogs line by line
        with open(args.in_csv) as in_file:
            for line_num, line in enumerate(in_file):
                # tokenize data fields
                line_toks = [tok.strip() for tok in line.split(',')] 
                
                if line_num == 0:
                    
                    data_field_idx['ip'] = line_toks.index('ip')
                    data_field_idx['date'] = line_toks.index('date')
                    data_field_idx['time'] = line_toks.index('time')
                    # cik, accession, and extention are not useful since in the FAQ it was 
                    # specified that multiple requests to the exactly same document within
                    # a session are still counted as different requests.
                    #data_field_idx['cik'] = line_toks.index('cik')
                    #data_field_idx['accession'] = line_toks.index('accession')
                    #data_field_idx['extention'] = line_toks.index('extention')            
                else:
                    new_req = parse_request(line_toks, data_field_idx)                
                        
                    # reached new time step; 
                    if new_req['date']+' '+new_req['time'] != curr_time and line_num != 1:
                        # go through all active sessions to check if any exceed max inactive period
                        ended_sessions = {}
                        for session in active_sessions.keys():
                            # get time diff from session last request to new req arrival
                            inactive_time = time_diff_sec(new_req['date']+' '+new_req['time'], 
                                                            active_sessions[session]['last_req_time'], time_fmt)                        
                            
                            # log ended session 
                            if inactive_time > max_inactive_period:
                                # ended sessions will be recorded first by order of first request time, then by 
                                # line number in input file
                                ended_sessions[session] = {'first_req_time':active_sessions[session]['first_req_time'],
                                                            'line_num': active_sessions[session]['line_num']}
                        if len(ended_sessions) > 0:
                            log_session(out_file, active_sessions, ended_sessions, time_fmt)

                        # update curr_time
                        curr_time = new_req['date']+' '+new_req['time']
                        
                    if new_req['ip'] not in active_sessions.keys():
                        create_user_session(active_sessions, new_req, line_num)
                    else: 
                        sess_last_time = active_sessions[new_req['ip']]['last_req_time']    
                        elapsed_time = time_diff_sec(new_req['date']+' '+new_req['time'], sess_last_time, time_fmt)
                        update_session_req(active_sessions, new_req, elapsed_time)

        # log remaining reqs
        log_session(out_file, active_sessions, active_sessions, time_fmt)
                
                 
                 
