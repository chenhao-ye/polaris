import re
import pandas as pd

clv = True
NUM_THREAD = 20
STATUS = {0:"commit", 2:"abort"}


locktable = {} # row_id: { owners: {}, waiters: {}, (opt) retired: {}}
barriers = {}
priority = {}
status = {}
schedule = pd.DataFrame()


match_bracket = re.compile(r"[\[](.*)[\]](.*)")
match_thread = re.compile(r"thread-(\d*)")
match_txn = re.compile(r"txn-(\d*)\s*[\(]*(\d*)[\)]*")
match_rowclv = re.compile(r"row_clv-(\d*)")
match_row = re.compile(r"row-(\d*)")
match_type = re.compile(r"[\(]type (\d)[\)]")
match_cohead = re.compile(r"is_cohead=(\d*)")
match_delta = re.compile(r"delta=(\d*)")

def get_thd(txn):
    return txn % NUM_THREAD

def rm_txn(row, place, txn):
    thread = get_thd(txn)
    members = locktable[row][place]
    found = False
    for i, step in enumerate(members):
        if step[0] == txn:
            found = True
            break
    if not found:
        print("not found: {}".format(line))
        raise
    locktable[row][place] = members[:i] + members[i+1:]
    return thread, members[i]

def get_schedule(start, end, thread):
    return schedule.loc[start:end+1, thread][schedule.loc[start:end+1, thread].notnull()]

def find_locktable_with_txn(txn, disp=False):
    found = False
    for row in locktable:
        if txn in [item[0] for place in ['owners', 'waiters', 'retired'] for item in locktable[row][place] ]:
            print(row)
            if disp:
                print(locktable[row])
            found = True
    return found
            
def find_locktable_with_row(row):
    print(locktable[row])
    
def get_p(txn):
    if txn in priority:
        p = priority[txn]
    else:
        p = -1
    return p

def extract_type(content):
    t = int(match_type.search(content).group(1))
    if (t == 0):
        return "EX"
    else:
        return "SH"
    
def init_row(row):
    if row not in locktable:
        locktable[row] = {}
        locktable[row]['owners'] = []
        locktable[row]['waiters'] = []
        locktable[row]['retired'] = []
        
def add_to(row, place, item):
    init_row(row)
    locktable[row][place].append(item)
    
def parse(line, ts): 
    if "[" not in line:
        return
    ts += 1
    # init variables
    thread = -1
    txn = -1
    p = -1
    row = ""
    # match [*] *
    matched = match_bracket.search(line)
    header = matched.group(1)
    content = matched.group(2)
    # match thread
    matched = match_thread.search(header)
    if (matched):
        thread = int(matched.group(1))
    # match txn
    matched = match_txn.search(header)
    if (matched):
        txn = int(matched.group(1))
        if (matched.group(2)):
            p = int(matched.group(2))
            priority[txn] = p
    # match row
    matched = match_rowclv.search(header)
    if (matched):
        row = matched.group(1)
    matched = match_row.search(header)
    if (matched):
        row = matched.group(1)
    # process content
    if row != "":
        if "add to waiters" in content:
            add_to(row, "waiters", (txn, p, extract_type(content)))
        elif "move to owners" in content:
            thread, item = rm_txn(row, "waiters", txn)
            add_to(row, "owners", item)
        elif "rm from" in content:
            thread, item = rm_txn(row, content.split()[-1], txn)
        elif "move to retired" in content:
            cohead = int(match_cohead.search(content).group(1)) == 1
            delta = int(match_delta.search(content).group(1)) == 1
            add_to(row, "retired", (txn, p, extract_type(content), cohead, delta))
        elif "wound txn" in content:
            schedule.loc[ts, get_thd(txn)] = content + " on %s"%row
            wounded = int(content.split()[2])
            schedule.loc[ts, get_thd(wounded)] = "wounded by %d on %s"%(txn, row) 
        elif "cannot find entry when trying to release" in content:
            error = False
            for key in locktable[row]:
                for item in locktable[row][key]:
                    if(item[0] == txn):
                        print("ERROR: find in %s (%s)\n" % (key, line))
                        error = True
            if error:
                schedule.loc[ts, get_thd(txn)] = "cannot find when releasing %d on %s"%(txn, row)
        elif "change delta" in content:
            cohead = int(match_cohead.search(content).group(1)) == 1
            delta = int(match_delta.search(content).group(1)) == 1
            for i, item in enumerate(locktable[row]["retired"]):
                if item[0] == txn:
                    locktable[row]["retired"][i] = (item[0], item[1], item[2], cohead, delta)
    elif txn != -1:
        if thread == -1:
            if "set ts" in line:
                schedule.loc[ts, get_thd(txn)] = "txn %d "%txn + content
                priority[txn] = p
            elif "set to" in content:
                schedule.loc[ts, get_thd(txn)] = "txn %d "%txn + content
                status[txn] = content.split()[-1]
            elif "increment barrier" in content:
                barriers[txn] += 1
                schedule.loc[ts, get_thd(txn)] = "txn %d "%txn + content
            elif "decrement barrier to" in content:
                barriers[txn] -= 1
                schedule.loc[ts, get_thd(txn)] = "txn %d "%txn + content
        else:
            barriers[txn] = 0
            priority[txn] = 0
            status[txn] = "running"
            schedule.loc[ts, thread] = "txn %d "%txn + content
    elif thread != -1:
        if "finish" in content:
            schedule.loc[ts, thread] = content
            txn = int(content.split()[-2])
#             print("\ntxn %d aborted, should have no entry in locktable"%txn)
            if find_locktable_with_txn(txn):
                print("ERROR: txn %d aborted, should have no entry in locktable\n"%txn)
            if "(aborted)" in line:
                barriers[txn] = 0
    return ts