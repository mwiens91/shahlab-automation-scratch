Bootstrap: debootstrap
OSVersion: bionic
MirrorURL: http://us.archive.ubuntu.com/ubuntu/

%files
    automate_me/* .
    requirements.txt .

%post
    mkdir /logs
    apt-get install -y software-properties-common
    apt-add-repository universe
    apt-get update
    apt-get install -y python-pip
    pip install -r requirements.txt
