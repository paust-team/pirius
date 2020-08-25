from subprocess import call, Popen, PIPE

def kf_publish(topic, file_path, num_data):
    cmd = ["./kf-producer-bench", topic, file_path, str(num_data)]
    p = Popen(cmd, cwd="../kafka/client/producer/", stdin=PIPE, stdout=PIPE, stderr=PIPE)
    start_produce_time, err = p.communicate()
    rc = p.returncode
    if rc != 0:
        print("error occured on running kafka producer")
        return -1
    
    return int(start_produce_time)
    
    
def kf_subscribe(topic, count):
    cmd = ["./kf-consumer-bench", topic, str(count)]
    p = Popen(cmd, cwd="../kafka/client/consumer/", stdin=PIPE, stdout=PIPE, stderr=PIPE)
    end_consume_time, err = p.communicate()
    rc = p.returncode
    if rc != 0:
        print("error occurred on running kafka consumer")
        print(err)
        return -1
    
    return int(end_consume_time)
        
def sq_publish(topic, file_path, num_data):
    cmd = ["./sq-producer-bench", topic, file_path, str(num_data)]
    p = Popen(cmd, cwd="../shapleQ/client/producer/", stdin=PIPE, stdout=PIPE, stderr=PIPE)
    start_produce_time, err = p.communicate()
    rc = p.returncode
    if rc != 0:
        print("error occured on running shapleq producer")
        print(start_produce_time)
        print(err)
        return -1
    
    return int(start_produce_time)

def sq_subscribe(topic, count):
    cmd = ["./sq-consumer-bench", topic, str(count)]
    p = Popen(cmd, cwd="../shapleQ/client/consumer/", stdin=PIPE, stdout=PIPE, stderr=PIPE)
    end_consume_time, err = p.communicate()
    rc = p.returncode
    if rc != 0:
        print("error occurred on running shapleq consumer")
        print(end_consume_time)
        print(err)
        return -1
    
    return int(end_consume_time)