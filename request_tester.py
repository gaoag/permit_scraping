from time import sleep
import requests


text = 'technical problems'
tries = 0
wait = 25
while True:
    r = requests.get('http://dbiweb.sfgov.org/dbipts/default.aspx?page=Permit&PermitNumber=9409803')
    if text in r.text:
        print('failure after ' + str(tries) + ' tries')
    else:
        tries += 1
        sleep(wait)
        continue
