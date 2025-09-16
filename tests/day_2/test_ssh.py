import os
import paramiko

# Example SSH credentials (or fetch from ENV)
SSH_USER = os.environ.get("SSH_USER")
SSH_PASS = os.environ.get("SSH_PASS")
SSH_HOST = os.environ.get("SSH_HOST")  # node IP
SSH_PORT = int(os.environ.get("SSH_PORT"))

# Create SSH client
client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

# Connect
client.connect(SSH_HOST, port=SSH_PORT, username=SSH_USER, password=SSH_PASS)

# Run a simple command to verify SSH
stdin, stdout, stderr = client.exec_command("uptime")
print("STDOUT:", stdout.read().decode())
print("STDERR:", stderr.read().decode())

# Close connection
client.close()
