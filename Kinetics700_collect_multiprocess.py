import os
import subprocess
import json
import time
import cv2
from multiprocessing import Process

def createFolder(path):
    if not os.path.isdir(path):
        os.mkdir(path)
        print("create folder: {}".format(path))

def trim_video(start, end, input_path, output_path):
    # "ffmpeg -i ~/Desktop/01.mp4 -ss 19.0 -t 10.0 -c:v libx264 -c:a copy -threads 1 -loglevel panic ~/Desktop/02.mp4"
    cmd = "ffmpeg -i {} -ss {} -t {} -c:v libx264 -c:a copy -threads 1 -loglevel panic {}".format(input_path, start, end-start, output_path)
    try:
        output = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as err:
        print("trim cmd error {}".format(input_path))

def download_by_url(url, out_path):
    #youtube-dl -f best -o tmp.mp4 --quiet --no-warning "https://www.youtube.com//watch?v=---0dWlqevI"
    cmd = 'youtube-dl -f best -o {} --quiet --no-warning "{}"'.format(out_path, url)
    try:
        output = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as err:
        print("download cmd error {}".format(url))


def save_txt(save_path, success, failure):
    with open(save_path+"success.json", "w") as fjson:
        fjson.write(json.dumps(success, indent=2))

    with open(save_path+"failure.json", "w") as fjson:
        fjson.write(json.dumps(failure, indent=2))


def _download_list(pname, d_list, save_root_path):

    print("start download, process name:{}".format(pname))

    # record list
    success_list = []
    failure_list = []

    # download
    buff = 0
    save_p = 0 # save pointer
    save_duration = 100 # save when how many download over.
    for vindex in range(len(d_list)):
        vdata = d_list[vindex]
        try:
            # initialize information
            vid, label, segment, vurl, vlen  = None, None, [None, None], None, None
            # loading information
            for key in vdata:
                vid = key
                label = vdata[key]["annotations"]["label"]
                segment = vdata[key]["annotations"]["segment"]
                vurl = vdata[key]["url"]
                vlen = vdata[key]["duration"]

            # create save path
            video_save_path = save_root_path + "/{}/{}.mp4".format(label, vid)

            tmp_video_path = "tmp_{}_{}.mp4".format(pname, buff) # buff : to avoid timeout error
            download_by_url(url=vurl, out_path=tmp_video_path) # download video by youtube-dl to tmp_path
            trim_video(start=segment[0], end=segment[1], input_path=tmp_video_path, output_path=video_save_path) #trim video by ffmpeg
            
            buff += 1
            # avoid overflow
            if buff%10 == 0:
                buff = 0

            if checking(video_save_path, goal_length=vlen):
                print("successful: {}".format(video_save_path))
                success_list.append(vdata) # record that successfuly download and trim.
            else:
                print("error~ this video will download again.(0)   {}".format(video_save_path))
                failure_list.append(vdata) # record that fail to download and trim.

            is_del = subprocess.check_output("rm -rf {}".format(tmp_video_path),
                                              shell=True, stderr=subprocess.STDOUT) # delete tmp mp4
        except:
            print("error~ this video will download again.(1)   {}".format(video_save_path))
            failure_list.append(vdata) # record that fail to download and trim.

        if save_p%save_duration == 0:
            save_txt(save_path="./record/{}_".format(pname), success=success_list, failure=failure_list)
            save_p = 0
            print(" success:{}  fail:{}".format(len(success_list), len(failure_list)))

        save_p += 1


    if "end" not in pname:
        print("not end, second time!")
        _download_list(pname=pname+"_end", d_list=failure_list, save_root_path=save_root_path)


def checking(video_save_path, goal_length):

    # 01 check mp4 file exist or not
    if not os.path.isfile(video_save_path):
        return False
    else:
        return True

    """ not include this part ."""
    # 02 check length of this video is correct
    cap = cv2.VideoCapture(video_save_path)
    #width = cap.get(3)
    #height = cap.get(4)
    vfps = cap.get(5)
    vframes = cap.get(7)
    secs = vframes/vfps

    if secs != goal_length:
        return False

    return True

def load_history(json_datas, video_save_path,record_path="./record/"):

    ori_num = len(json_datas)

    files = os.listdir(record_path)
    actions = os.listdir(video_save_path)

    success_count = 0

    for action in actions:
        print("load history... {}".format(action))
        videos = os.listdir(video_save_path+"/"+action)
        for v in videos:
            if v.split(".")[-1] != "mp4":
                print("wrong file. {}".format(action+"/"+v))
                continue
            vid = v.split(".")[0]
            if vid in json_datas:
                del json_datas[vid]
                success_count += 1
            else:
                print("error when load successful data.   id : {}".format(vid))

    """
    failure_count = 0
    success_count = 0
    for file in files:
        record_file = open(record_path+"/"+file, "r")
        records = json.load(record_file)
        if "success" in file:
            print("load successful history : {}".format(file))
            # remove from video's list
            for r in records:
                for vid in r:
                    if vid in json_datas:
                        del json_datas[vid]
                    else:
                        print("error when load successful data.   id : {}".format(vid))
                success_count += 1 

        elif "failure" in file:
            print("load failure history : {}".format(file))
            # need to be
            for r in  records:
                failure_count += 1
        record_file.close()
    """

    print("check load : {}".format(len(json_datas)==(ori_num - success_count)))
    print("last time: {} successfully download. {} fail.".format(success_count, "-"))

    return json_datas


def allocate_jobs(json_datas, worker_num, save_root_path):

    worker_list = []
    total_list = []

    # create list
    for video_id in json_datas:
        tmp = {video_id:json_datas[video_id]}
        tmp[video_id]["finish"] = False
        tmp[video_id]["annotations"]["label"] = tmp[video_id]["annotations"]["label"].replace(" ", "_")
        total_list.append(tmp)
        # create action folder
        class_path = save_root_path+"/{}/".format(tmp[video_id]["annotations"]["label"])
        createFolder(class_path)

    print("\ncreate saving folder: {}/700.\n".format(len(os.listdir(save_root_path))))
    print("total data: {}".format(len(total_list)))

    # allocate list to workers
    sp = 0 # start pointer
    l = (int)(len(total_list)/worker_num)
    for w in range(worker_num):
        d_list = total_list[sp:sp+l]
        # w : process name, d_list : data list
        child_process = Process(target=_download_list, args=(w, d_list, save_root_path))
        worker_list.append(child_process)   
        sp = sp + l # next data

    # start process
    for w in range(worker_num):
        print("attempt to start child {} ...".format(w))
        worker_list[w].start()
        print("           start successfully!")

    return worker_list


def main(json_path, save_root_path):

    # read json data
    fjson = open(json_path, "r")
    json_datas = json.load(fjson)
    fjson.close()

    worker_num = (int)(input("your cpu ?\nHow many thread in there?(hint: i7->8)\n : "))

    json_datas = load_history(json_datas, save_root_path) # load history data

    # start assign jobs for childs
    worker_list = allocate_jobs(json_datas, worker_num, save_root_path)

    # checking
    for w in range(worker_num):
        worker_list[w].join()


if __name__ == "__main__":

    """

    Step1. Check your python version 3.x
    
    Step2. install ffmpeg : 

        sudo apt-get install ffmpeg

    Step4. install youtube-dl : 
        
        sudo wget https://yt-dl.org/latest/youtube-dl -O /usr/local/bin/youtube-dl
        sudo chmod a+x /usr/local/bin/youtube-dl
        hash -r

    """

    createFolder(path="./record/")

    json_path = input("please input json file path\n : ")
    save_path = input("please input video save path\n : ")
    #save_path = "/media/chou/HP P600/"
    createFolder(path=save_path)

    print("\n\n\n")
    main(json_path, save_path)
    print("\n\n\ndone.")

