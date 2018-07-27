Bootstrap: debootstrap
OSVersion: xenial
MirrorURL: http://us.archive.ubuntu.com/ubuntu/

%

%files
    automate_me /shahlab_automation
    requirements.txt /shahlab_automation/

%post
    mkdir /logs

    apt-get install -y software-properties-common
    apt-add-repository universe
    apt-get update
    apt-get install -y python-pip

    cd /shahlab_automation
    pip install -r requirements.txt
