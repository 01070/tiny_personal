# -- coding: utf-8 --
# @Time : 2023/5/23 11:13
# @Author : Yao Sicheng
from flask import Flask, render_template, flash, request
from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField
from demo_main import main

app = Flask(__name__)
app.secret_key = "scyao"


class NerForm(FlaskForm):
    database = StringField("数据库")
    table = StringField('表名')
    submit = SubmitField(u"开始执行")

@app.route('/', methods=["GET", "POST"])
def identify():  # put application's code here
    form = NerForm()
    if request.method == "POST":
        database = request.form.get("database")
        table = request.form.get('table')
        main(database, table)

        flash('success')

    return render_template("index.html", form=form)

def select_table():
    pass


if __name__ == '__main__':
    app.run(host="0.0.0.0")
