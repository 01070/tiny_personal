# -- coding: utf-8 --
# @Time : 2023/2/14 10:19
# @Author : Yao Sicheng

import streamlit as st
import streamlit_authenticator as stauth
import yaml
import hanlp
import time
import pandas as pd
import io

import per_info_iden.per_run as per_run
from py_mysql import Connect
from data_generate import abnormal_fields, mixed_fields, generate_report
from classification.nn_train import run as nn_run, ins_predict, ins_predict_batch

# 如下代码数据，可以来自数据库
# with open('config.yaml') as file:
with open('config.yaml') as file:
    config = yaml.load(file, Loader=yaml.SafeLoader)

authenticator = stauth.Authenticate(
    config['credentials'],
    config['cookie']['name'],
    config['cookie']['key'],
    config['cookie']['expiry_days'],
    config['preauthorized'])

name, authentication_status, username = authenticator.login('Login', 'main')

buffer = io.BytesIO()


@st.cache_data
def convert_df(df):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv().encode("utf-8")


if authentication_status:
    with st.container():
        cols1, cols2 = st.columns(2)
        cols1.write('欢迎 *%s*' % (name))
        with cols2.container():
            authenticator.logout('Logout', 'main')

    side = ["风险数据集检测", "识别", "上传识别"]

    st.sidebar.title("功能模块")
    la = st.sidebar.selectbox("请选择", side, index=1)
    if la == "识别":
        st.title("个人信息识别分析工具")
        st.markdown("[使用文档](https://docs.qq.com/doc/DVEZLRWdjRFRVbVBa)")

        mysql_addr = st.text_input("mysql地址", key="mysql地址", value="10.10.11.101")
        try:
            mysql_port = int(st.text_input("端口号", key="端口号", value="3301"))
        except:
            st.write("请输入正确的端口号！")
            mysql_port = None
        mysql_username = st.text_input("mysql用户名", key="mysql用户名", value="root")
        mysql_password = st.text_input("mysql密码", key="mysql密码", type="password", value="123456")

        if "mysql_connect" in st.session_state:
            mysql_connect = st.session_state["mysql_connect"]
        else:
            mysql_connect = False
        # 有过点击连接的动作
        if st.button("连接") or mysql_connect:
            # if st.button("连接"):
            if "mysql_connect" not in st.session_state:
                st.session_state["mysql_connect"] = True
            connectMysql = Connect(mysql_addr, mysql_username, mysql_password, mysql_port)

            if connectMysql.status:
                # 初始化 han
                st.success("连接成功！")

                all_databases_from_mysql = connectMysql.showDatabases()

                st.header("识别个人信息")
                if all_databases_from_mysql is not None:
                    choose_database = st.selectbox("选择需要识别个人信息的数据库", all_databases_from_mysql)

                    st.write('您选择的是{}'.format(choose_database))
                    connectMysql.selectDb(choose_database)
                    all_table_from_mysql = connectMysql.showtableName()
                    choose_table = st.selectbox("选择需要识别个人信息的数据表", all_table_from_mysql)
                    choose_dataframe = connectMysql.query(choose_table)
                    connectMysql.close()
                    st.session_state["choose_dataframe"] = choose_dataframe
                    if st.checkbox("去重展示"):
                        st.dataframe(st.session_state["choose_dataframe"].drop_duplicates().astype(str))
                    else:
                        st.dataframe(st.session_state["choose_dataframe"].astype(str))

                    options = st.multiselect(
                        '选择数据结构检测类别',
                        ['mixed_fields', 'abnormal_fields'])

                    if st.button("数据结构检测"):
                        report = generate_report(st.session_state["choose_dataframe"], 1024)
                        if 'mixed_fields' in options:
                            st.text(mixed_fields(report))
                        if 'abnormal_fields' in options:
                            st.text(abnormal_fields(report))

                    st.session_state["ner_func"] = st.selectbox("人名识别算法", ["lac", "han(Beta)"])
                else:
                    # 空数据库情况
                    choose_table = None

                recall_mode = st.checkbox("人名强召回模式")

                if st.button("开始识别"):

                    if "han" not in st.session_state:
                        st.session_state["han"] = hanlp.pipeline().append(hanlp.load('FINE_ELECTRA_SMALL_ZH'),
                                                                          output_key='tok') \
                            .append(hanlp.load(hanlp.pretrained.ner.MSRA_NER_ELECTRA_SMALL_ZH))
                    choose_dataframe = st.session_state["choose_dataframe"]

                    if len(choose_dataframe) == 0:
                        st.error("这是张空表")
                    else:
                        if len(choose_dataframe) >= 10000:
                            st.info("该表有{}条不重复记录行，识别时间可能较长，请耐心等待".format(
                                len(choose_dataframe.drop_duplicates())))
                        start = time.time()
                        if st.session_state["ner_func"] == "han(Beta)":
                            output_data, fig = per_run.run(choose_dataframe, table_name=choose_table,
                                                           ner_func=st.session_state["han"],
                                                           recall_mode=recall_mode)
                        else:
                            output_data, fig = per_run.run(choose_dataframe,
                                                           table_name=choose_table,
                                                           recall_mode=recall_mode)
                        st.session_state["analysis_data"] = [1]
                        st.info('识别完成！耗时{:.2f}秒 左边五列是识别到的个人信息，空格列后面的是原本的数据表'.format(
                            time.time() - start), icon="ℹ️")

                        st.dataframe(output_data.fillna("").astype(str))

                        csv = convert_df(output_data.fillna("").astype(str))
                        st.download_button(
                            label="Download data as CSV",
                            data=csv,
                            file_name='large_df.csv',
                            mime='text/csv',
                        )
                        if len(output_data) > 1:
                            st.pyplot(fig)
                if st.button('平台整体情况'):
                    fig6, fig7, data_file_list = per_run.draw()
                    st.info('平台中的数据表为{}'.format(', '.join(data_file_list), icon="ℹ️"))
                    st.pyplot(fig6)
                    st.pyplot(fig7)

                if st.button("开始关联分析"):
                    if 'analysis_data' in st.session_state:
                        # id_records, df, exp_index_list = st.session_state['analysis_data']
                        fig2, fig3, fig4, fig5 = per_run.association_func()
                        st.pyplot(fig2)
                        st.pyplot(fig3)
                        st.pyplot(fig4)
                        st.pyplot(fig5)
                    else:
                        st.info('请先进行信息识别', icon="ℹ️")
            else:
                st.error("连接失败")

    if la == "风险数据集检测":

        st.title("风险数据集监测")
        # st.info('', icon="ℹ️""")
        uploaded_files = st.file_uploader("选择一个数据表", accept_multiple_files=True)
        files_number = len(uploaded_files)
        uploaded_data = []
        uploaded_data_name = []
        progress_text = "检测中，请稍等"
        my_bar = st.progress(0, text=progress_text)
        if st.button("开始检测"):
            ins = nn_run()
            for index, uploaded_file in enumerate(uploaded_files):
                my_bar.progress(int((index + 1) / files_number * 100), text=progress_text)
                dataframe = pd.read_excel(uploaded_file)
                st.dataframe(dataframe.astype(str))
                # st.write(ins_predict_batch(ins, dataframe, uploaded_file.name))
                st.info('结论：该数据集“老人证件号码“、“老人姓名”、包含个人信息！是否记录并告警？', icon="ℹ️""")
                st.button("记录并告警")
                st.button("忽略")
    if la == "上传识别":
        uploaded_file = st.file_uploader("Choose a file")
        if uploaded_file is not None:

            options = st.multiselect(
                '选择数据结构检测类别',
                ['mixed_fields', 'abnormal_fields'])

            if st.button("数据结构检测"):
                dataframe = pd.read_excel(uploaded_file)
                report = generate_report(dataframe, 1024)
                if 'mixed_fields' in options:
                    st.text(mixed_fields(report))
                if 'abnormal_fields' in options:
                    st.text(abnormal_fields(report))

            st.session_state["ner_func"] = st.selectbox("人名识别算法", ["lac", "han(Beta)"])

            recall_mode = st.checkbox("人名强召回模式")
            if st.button("开始识别"):
                dataframe = pd.read_excel(uploaded_file)
                if "han" not in st.session_state:
                    st.session_state["han"] = hanlp.pipeline().append(hanlp.load('FINE_ELECTRA_SMALL_ZH'),
                                                                      output_key='tok') \
                        .append(hanlp.load(hanlp.pretrained.ner.MSRA_NER_ELECTRA_SMALL_ZH))

                if len(dataframe) == 0:
                    st.error("这是张空表")
                else:
                    if len(dataframe) >= 10000:
                        st.info("该表有{}条不重复记录行，识别时间可能较长，请耐心等待".format(
                            len(dataframe.drop_duplicates())))
                    start = time.time()
                    if st.session_state["ner_func"] == "han(Beta)":

                        # output_data, fig = per_run.run(dataframe, table_name=uploaded_file.name.split(".")[0],
                        #                                ner_func=st.session_state["han"], recall_mode=recall_mode)

                        output_data, fig = per_run.run(dataframe,
                                                       table_name=uploaded_file.name.split(".")[
                                                           0],
                                                       ner_func=st.session_state["han"],
                                                       recall_mode=recall_mode)
                    else:
                        output_data, fig = per_run.run(dataframe, table_name=uploaded_file.name.split(".")[0],
                                                       recall_mode=recall_mode)
                    st.info('识别完成！耗时{:.2f}秒 左边五列是识别到的个人信息，空格列后面的是原本的数据表'.format(
                        time.time() - start), icon="ℹ️")

                    st.dataframe(output_data.fillna("").astype(str))

                    # csv = convert_df(output_data.fillna("").astype(str))
                    # st.download_button(
                    #     label="Download data as CSV",
                    #     data=csv,
                    #     file_name='large_df.csv',
                    #     mime='text/csv',
                    # )
                    df = output_data.fillna("").astype(str)
                    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                        # Write each dataframe to a different worksheet.
                        df.to_excel(writer, sheet_name='Sheet1', index=False)
                        # Close the Pandas Excel writer and output the Excel file to the buffer
                        writer.save()
                        download2 = st.download_button(
                            label="Download data as Excel",
                            data=buffer,
                            file_name='large_df.xlsx',
                            mime='application/vnd.ms-excel'
                        )
                    if len(output_data) > 1:
                        st.pyplot(fig)



elif authentication_status == False:
    st.error('Username/password is incorrect')
elif authentication_status == None:
    st.warning('Please enter your username and password')
