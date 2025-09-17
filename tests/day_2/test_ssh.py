import os
import paramiko
from pathlib import Path

SSH_USER = os.environ.get("SSH_USER")
SSH_PASS = os.environ.get("SSH_PASS")
SSH_HOST = os.environ.get("SSH_HOST")  # node IP
SSH_PORT = int(os.environ.get("SSH_PORT", 22))


def run_ssh_cmd(cmd: str, out_file: Path = None) -> str:
    """Run a command via SSH and optionally save output to file."""
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(SSH_HOST, port=SSH_PORT, username=SSH_USER, password=SSH_PASS)

    stdin, stdout, stderr = client.exec_command(cmd)
    if SSH_PASS:
        try:
            stdin.write(f"{SSH_PASS}\n")
            stdin.flush()
        except Exception:
            pass

    out = stdout.read().decode()
    err = stderr.read().decode()
    client.close()

    if out_file:
        out_file.write_text(out + err)

    return out + err


# Quick test if run standalone
if __name__ == "__main__":
    print(run_ssh_cmd("uptime"))

