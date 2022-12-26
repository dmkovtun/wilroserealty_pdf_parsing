from os import popen, getcwd, chdir
from os.path import join

print("Started setup and login process.")

current_dir = getcwd()

login_dir = join(current_dir, "data/credentials/fixture")
chdir(login_dir)


def run_cmds(cmds):
    for cmd in cmds:
        stream = popen(cmd)
        output = stream.read()


run_cmds(["poetry install", "poetry run python get_token.py"])
print("Login completed successfully.")

chdir(current_dir)
run_cmds(["docker-compose build", "docker-compose up -d"])
print("Docker image setup is completed.")
