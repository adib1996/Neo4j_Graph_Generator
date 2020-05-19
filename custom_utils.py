import sys
import time

def get_time_string(x):
    if x > 24*60*60:
        ndays = int(x/24*60*60)
        if ndays == 1:
            return "more than %d day"%ndays
        else:
            return "more than %d days"%ndays
    else:
        timeStr = ""
        if int(time.strftime("%H", time.gmtime(x))) != 0:
            timeStr = "%d Hour"%(int(time.strftime("%H", time.gmtime(x))))
        if int(time.strftime("%M", time.gmtime(x))) != 0:
            if timeStr == "":
                timeStr = "%d Min"%(int(time.strftime("%M", time.gmtime(x))))
            else:
                timeStr = timeStr + " " + "%d Mins"%(int(time.strftime("%M", time.gmtime(x))))
        if int(time.strftime("%S", time.gmtime(x))) != 0:
            if timeStr == "":
                timeStr = "%d Sec"%(int(time.strftime("%S", time.gmtime(x))))
            else:
                timeStr = timeStr + " " + "%d Secs"%(int(time.strftime("%S", time.gmtime(x))))
        else:
            if ((int(time.strftime("%H", time.gmtime(x))) == 0) & (int(time.strftime("%M", time.gmtime(x))) == 0)):
                timeStr = "1 Sec"
        return timeStr

# Custom staus bar code
def custom_status_bar(n, N, start_time, current_time):
    if ((n == 1) | (n == N)):
        time_rem_str = ""
    else:
        # timeRemStr = "(Remaining : %0.1f sec)"%(((current_time-start_time)*(N-n)*1.0)/n)
        time_rem_str = "(Remaining: %s)"%(get_time_string(((current_time-start_time)*(N-n)*1.0)/n))
    sys.stdout.write('\r')
    sys.stdout.write("(%d/%d) %d%%  %s" % (n, N, 100.0*n/N, time_rem_str))
    sys.stdout.write("\033[K")
    sys.stdout.flush()
