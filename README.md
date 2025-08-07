# Isotop Stock Ticker
This will update the list of stocks from 9.00 am to 05.00 pm every day every 5 minutes for IDX Exchange.

## Hosting notes:
### Amazon EC2 instance id: i-07482b99271a785b5
### This script has already been hosted in amazon AWS EC2. Ensure to also change the amazon ec2 script. Here's how to do it:
1. go to https://ap-southeast-1.console.aws.amazon.com/ec2/home?region=ap-southeast-1#InstanceDetails:instanceId=i-07482b99271a785b5
2. click connect. It should bring you to the terminal
3. run ```sudo su``` to get to the root user
4. ```cd isotop-learn``` to navigate to isotop learn
5. ```ps aux | grep python3```
6. ```kill [number next to root]```: Kill the mainscript.py thread. DO NOT KILL grep --color=auto python3
7. ```nano mainscript.py```: Open mainscript.py
8. Copy the changes and paste to mainscript.py, then ```command + s``` -> ```command + x```
9. ```nobup python3 -u mainscript.py &```

## Some command guides:
- ```ls```: Check files in the folder,
- ```nobup python3 -u mainscript.py &```: Run script in background.
- ```tail -f nohup.out```: Check if script it working behind background.
- ```ps aux | grep python3```: See threads running in background.
- ```kill [number next to root]```: Kill thread running in background.

