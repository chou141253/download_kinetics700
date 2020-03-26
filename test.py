import subprocess

cmd = "python3 {}".format("test2.py")
out = subprocess.check_output(cmd,shell=True, stderr=subprocess.STDOUT)
print(out.decode())