from typing import Any
from flask import Flask, request, redirect, session, url_for, flash, get_flashed_messages, render_template
from markupsafe import Markup
import os
import os.path
import sys
from datetime import datetime
from sqlitedict import SqliteDict
import json
import requests
import signal

app = Flask(__name__)
app.secret_key = '2e95cd95a740a13fb6df15a12f1fa4ca58980f95d8612ac44dfa0be45562f47d'
db = SqliteDict("db.sqlite")

BASE_URL = "https://go.mynt.in"
algo_users = []

def read_users():
    global algo_users
    algo_users = []
    home = os.path.expanduser('~')
    filename = f'{home}/users.txt'
    print(filename)
    if os.path.exists(filename):
        with open(filename, 'r') as file:
            lines = file.readlines()
            algo_users = [line.strip() for line in lines if line.strip()]

def post_data(session_token: str, uid: str) -> Any:
    url = BASE_URL + "/NorenWClientTP/PositionBook"
    data = {"uid": uid,
            "actid": uid,
            }
    payload = "jData=" + json.dumps(data) + "&jKey=" + session_token
    headers = {'Content-Type': 'application/json'}
    response = requests.post(url, headers=headers, data=payload)
    jsondata = json.loads(response.text)

    if response.status_code == 200:
        return jsondata
    else:
        print(response.text)
        print(response.status_code)
        return jsondata

def get_uid(username: str) -> str:
    home = os.path.expanduser('~')
    filename = f'{home}/{username}/config.txt'
    print(filename)
    if os.path.exists(filename):
        with open(filename, 'r') as file:
            lines = file.readlines()
            for line in lines:
                if line.startswith('userid'):
                    return line.split('=')[1].strip()

@app.route('/position/<user>')
def position(user):
    today_date = str(datetime.today().date())
    home = os.path.expanduser('~')
    filename = f'{home}/{user}/db.sqlite'
    userdb = SqliteDict(filename)

    if 'session_token' in userdb and 'last_login_date' in userdb and userdb['last_login_date'] == today_date:
        session_token = userdb['session_token']
        uid = get_uid(user)
        data = post_data(session_token, uid)
        print(session_token, uid)
        msg = ""
        items = []
        # print(data)
        if 'stat' in data and data['stat'] == 'Not_Ok':
            msg = f"errror: {data['emsg']}"
        else:
            items = data
            # print(data)
            for item in data:
                if item['daybuyqty'] != "":
                    item['daybuyvalue'] = float(item['daybuyavgprc']) * float(item['daybuyqty'])
                    item['daysellvalue'] = float(item['daysellavgprc']) * float(item['daysellqty'])
                else:
                    item['daybuyvalue'] = 0
                    item['daysellvalue'] = 0
        return render_template('position.html', items=data, username=user, uid=uid, msg=msg)
        # data = post_data(session_token, )
    else:
        return render_template('position.html', items=[], username=user, uid="", msg="unable to fetch orders")

@app.route('/position/json/<user>')
def positionjson(user):
    today_date = str(datetime.today().date())
    home = os.path.expanduser('~')
    filename = f'{home}/{user}/db.sqlite'
    userdb = SqliteDict(filename)

    if 'session_token' in userdb and 'last_login_date' in userdb and userdb['last_login_date'] == today_date:
        session_token = userdb['session_token']
        uid = get_uid(user)
        data = post_data(session_token, uid)
        for item in data:
            if item['daybuyqty'] != "":
                item['daybuyvalue'] = float(item['daybuyavgprc']) * float(item['daybuyqty'])
                item['daysellvalue'] = float(item['daysellavgprc']) * float(item['daysellqty'])
            else:
                item['daybuyvalue'] = 0
                item['daysellvalue'] = 0
        return data
    else:
        return "unable to fetch orders"

@app.route('/')
def index():
    msg = get_flashed_messages()
    if msg:
        return msg[0]

    logged_in = False

    if 'username' in session:
        logged_in = True

    return render_template('index.html', logged_in=logged_in, msg=msg)


@app.route('/users')
def users():
    read_users()
    if 'username' not in session:
        flash('user not logged in')
        return redirect(url_for('login'))

    return render_template('users.html', users=algo_users)

    # userlist = [f"<li>{user} <a href='/config/{user}'>config</a> | <a href='/logs/{user}/100'>logs</a> | <a href='/position/{user}'>position</a></li>" for user in algo_users]
    # return f"""
    #     <ul>
    #         {''.join(userlist)}
    #     </ul>
    #     <a href="/">back</a>
    # """

def get_config(user):
    home = os.path.expanduser('~')
    filename = f'{home}/{user}/config.toml'
    if os.path.exists(filename):
        with open(filename, 'r') as file:
            return file.read()
    return ""

@app.route('/config/<user>', methods=['GET', 'POST'])
def config(user):
    if 'username' not in session:
        flash('user not logged in')
        return redirect(url_for('login'))

    msg = ""
    """ returns output """
    if request.method == 'GET':
        config = get_config(user)
        # print(config)
        # return render_template('config.html', user=user, config=config)
    else:
        config: str = request.form['config']
        # config = '\n'.join(line.strip() for line in config.splitlines() if line.strip())
        if config:
            home = os.path.expanduser('~')
            with open(f'{home}/{user}/config.toml', 'w') as f:
                f.write(config)
            c = config.replace('\n', '<br>')
            msg = f"config saved for user: {user}"
        else:
            msg = "config not found"
    return render_template('config.html', user=user, config=config, msg=msg)


@app.route('/stop/<user>', methods=['GET', 'POST'])
def stop(user):
    if 'username' not in session:
        flash('user not logged in')
        return redirect(url_for('login'))

    stopped = False
    msg = ""
    """ returns output """
    if request.method == 'GET':
        pass
        # config = get_config(user)
        # print(config)
        # return render_template('config.html', user=user, config=config)
    else:
        home = os.path.expanduser('~')
        pid_file_path = f'{home}/{user}/pid.txt'
        # pid_file_path = "D:/repo/supertrend/pid.txt"
        print(pid_file_path)
        if os.path.exists(pid_file_path):
            saved_pid = 0
            with open(pid_file_path, 'r') as pid_file:
                saved_pid = int(pid_file.read())

            if saved_pid != 0:
                os.kill(saved_pid, signal.SIGTERM)
                stopped = True
                msg = "ALGO STOPPED!!!"
            else:
                msg = "error: pid 0"
        else:
            msg = "error: pid file not exist"

    return render_template('stop.html', stopped=stopped, user=user, msg=msg)


@app.route("/logs/<user>/<int:nlines>", methods=['GET'])
def logs(user, nlines):
    """ returns output """
    home = os.path.expanduser('~')
    with open(f'{home}/{user}/output.txt', 'r') as file:
        lines = file.readlines()
        nr_lines = nlines or 50
        last_n_lines = lines[-(nr_lines):]
        last_n_lines = [Markup.escape(line) for line in last_n_lines]
        last_n_lines = [str(line) + "<br>" for line in last_n_lines]
        return render_template('output.html', output=''.join(last_n_lines))

def get_scrips() -> str:
    home = os.path.expanduser('~')
    if os.path.exists(f'{home}/trade.txt'):
        with open(f'{home}/trade.txt', 'r') as f:
            return f.read()
    return ""

@app.route("/scrip", methods=['GET', 'POST'])
def scrip():
    scrips = ""
    msg = ""

    """ returns output """
    if request.method == 'GET':
        scrips = get_scrips()
    else:
        if 'username' not in session:
            flash('user not logged in')
            return redirect(url_for('login'))


        scrips: str = request.form['scrips']
        scrips = '\n'.join(line.upper().strip() for line in scrips.splitlines() if line.upper().strip())
        if scrips:
            home = os.path.expanduser('~')
            with open(f'{home}/trade.txt', 'w') as f:
                f.write(scrips)
            with open(f'{home}/pre-open-date.txt', 'w') as f:
                f.write(f'{datetime.today().date().isoformat()}')
            s = scrips.replace('\n', '<br>')
            msg = f"scrips saved"
        else:
            msg = "enter scrips"
    return render_template('scrip.html', scrips=scrips, msg=msg)

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        # TODO: username and password is hard-coded here, change it to load from somewhere 
        if request.form['username'] == 'admin' and request.form['password'] == 'set-your-password-here':
            session['username'] = 'admin'
        else:
            flash("invalid username password")
        return redirect(url_for('scrip'))
    msg = get_flashed_messages() or ['']
    return render_template('login.html', msg=msg[0])

@app.route('/logout')
def logout():
    # remove the username from the session if it's there
    session.pop('username', None)
    return redirect(url_for('index'))


def main():
    with open('output.txt', 'r') as file:
        lines = file.readlines()
        last_n_lines = lines[-50:]
        last_n_lines = [Markup.escape(line) for line in last_n_lines]
        last_n_lines = [line for line in last_n_lines]
        output = ""
        for l in last_n_lines:
            output = output + str(l) + "<br>"
        # print(''.join(last_n_lines))
        print(output)



if __name__ == '__main__':
    read_users()
    print(algo_users)
    app.run(debug=True, host='0.0.0.0', port=5000)
    # main()
