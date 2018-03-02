#!/usr/bin/env python
# -*- coding:utf-8 -*-

import ConfigParser
import optparse
import os,sys
import time
import commands
import tarfile
import json
#七牛sdk
from qiniu import Auth, put_file, BucketManager
#tencent SDK
from qcloud_cos import CosConfig
from qcloud_cos import CosS3Client
#邮件发送相关依赖
from email.header import Header
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import parseaddr, formataddr
import smtplib
#脚本目录
ROOT = os.path.split(os.path.realpath(__file__))[0]
SELF = os.path.basename(__file__)
TMP_DIR = '/tmp'
TMP_DIR = TMP_DIR if os.access(TMP_DIR, os.F_OK) and os.access(TMP_DIR, os.W_OK) else ROOT
LOG_FILE = ROOT + '/' + SELF + '.log'
SERVER = ''
#默认配置文件
DEFAULT_INI = ROOT + '/alibak.ini'
CONFIG = {}

def bakDB(db_name):
    db_name = db_name.split(',')
    try:
        db_conf = CONFIG['db']
        status, mysqldump = commands.getstatusoutput('which mysqldump')
        if status != 0:
            raise Exception('mysqldump命令不存在:'+mysqldump)
        file = "%s.%s.sql" % ('-'.join(db_name), time.strftime('%Y%m%d-%H%M%S'))
        dump_file = "%s/%s" % (TMP_DIR, file)  # 备份名称

        cmd = "%s -h%s -u%s -p'%s' -f %s --lock-all-tables > %s"
        cmd = cmd % (mysqldump, db_conf['host'], db_conf['user'], db_conf['password'],
                     (db_name and ' '.join(['-B'] + db_name) or '--all-databases'),
                     dump_file)
        log(cmd, send=False)
        status, output = commands.getstatusoutput(cmd)
        if status != 0:
            title = 'mysqldump命令执行失败'
            log(title + ': ' + cmd, send=False)
            raise Exception(title + ': ' + output + ' (执行命令查看日志)')
        #数据库文件压缩一下处理
        status, output = commands.getstatusoutput('gzip %s' % dump_file)
        if status != 0:
            raise Exception('sql压缩失败:' + output)
        return upload('%s.gz'%dump_file, 'mysql/%s.gz'%file)
    except Exception as e:
        title = '备份数据库'+','.join(db_name)+'失败'
        log(title + ':'+str(e), title=title)
        return False

'''
    备份文件或者目录，目录会自动tar打包
    上传到七牛的时候会自动文件名后面打上日期
'''
def bakDirOrFile(file_name):
    try:
        if os.path.exists(file_name) == False :
            raise Exception('需要备份的：' + file_name + '不存在')
        #给备份文件打上时间戳
        fix_date = time.strftime('%Y%m%d-%H%M%S')
        if os.path.isdir(file_name) :
            key = 'dir/' + os.path.basename(file_name) + '-' + fix_date + '.tar.gz'
            bak_file = TMP_DIR + '/' + key
            #一次性打包整个根目录。空子目录会被打包。如果只打包不压缩，将"w:gz"参数改为"w:"或"w"即可。
            with tarfile.open(bak_file, "w:gz") as tar:
                tar.add(file_name, arcname=os.path.basename(file_name))
            log(file_name+' => '+ bak_file, send=False)
        else:
            key = 'file/' + os.path.basename(file_name)
            if key.find('.') < 0:
                key = key + '-' + fix_date
            else:
                key = key.replace('.', '-' + fix_date + '.', 1)
            log(file_name + ' => ' + key, send=False)
            bak_file = file_name
        return upload(bak_file, key)
    except Exception as e :
        title = '备份文件或者目录'+file_name+'失败'
        log(title + ':'+str(e), title=title)
        return False

'''
    读取ini配置文件
'''
def readIni(ini_file):
    conf = ConfigParser.ConfigParser()
    conf.read(ini_file)

    opts = {}
    for x in conf.sections():
        for y in conf.options(x):
            value = conf.get(x, y)
            if not opts.has_key(x) :
                opts[x] = {}
            opts[x][y] = value if value.find(',') < 0 else value.split(',')
    return opts

'''
    记录日志
'''

def log(msg, **kwargs):
    #执行的上下文信息
    try:
        content = json.dumps(kwargs['content']) if kwargs.has_key('content') else '{' + ' '.join(sys.argv) + '}'
        error_msg = '[%s] %s %s \r\n' % (time.strftime('%Y-%m-%d %H:%M:%S'), msg, content)
        with open(LOG_FILE, 'a') as f:
            f.write(error_msg)
        # 默认发送邮件通知
        send = kwargs['send'] if kwargs.has_key('send') else True
        title = kwargs['title'] if kwargs.has_key('title') else None
        if send:
            send_mail(error_msg, title)
    except Exception as e:
        title = '日志文件写入失败'
        flag = send_mail(title + ': ' + str(e), title=title, wlog=False)
        if flag == False:
            print "邮件发不出去，日志写不进去，你想咋样 -->" + str(e)
            exit(1)

'''
格式化
'''
def _format_addr(s):
    name, addr = parseaddr(s)
    return formataddr((Header(name, 'utf-8').encode(), addr.encode('utf-8') if isinstance(addr, unicode) else addr))

'''
发送邮件
'''
def send_mail_file(file):
    email_conf = CONFIG['email']
    msg = MIMEMultipart('related')
    msg['Subject'] = Header(u'数据库备份', 'utf-8').encode()
    # 构造附件 读取文件作为附件，open()要带参数'rb'，使文件变成二进制格式,从而使'base64'编码产生作用，否则附件打开乱码
    att = MIMEText(open(file, 'rb').read(), 'base64', 'utf-8')
    att["Content-Type"] = 'application/octet-stream'
    att["Content-Disposition"] = 'attachment; filename="%s"' % os.path.basename(file)
    msg.attach(att)
    msg['From'] = _format_addr(u'数据库备份 <%s>' % email_conf['from_addr'])
    msg['To'] = ','.join(email_conf['to_addr'])
    # 链接服务器并发送 阿里云服务器要ssl才行
    server = smtplib.SMTP_SSL(email_conf['smtp_server'], email_conf['smtp_port'])
    #server.set_debuglevel(1)
    server.login(email_conf['from_addr'], email_conf['password'])
    server.sendmail(email_conf['from_addr'], email_conf['to_addr'], msg.as_string())
    server.quit()

'''
    发送邮件消息，加上wlog是否记录日志参数，避免日志写不进去，邮件发不出去时候出现死循环
'''
def send_mail(message, title=None, wlog=True):
    try:
        email_conf = CONFIG['email']
        title = title if title else u'阿里云备份'
        msg = MIMEText(message, 'plain', 'utf-8')
        msg['Subject'] = Header(title, 'utf-8').encode()
        msg['From'] = _format_addr('%s <%s>' % (title, email_conf['from_addr']))
        msg['To'] = ','.join(email_conf['to_addr'])
        # 链接服务器并发送 阿里云服务器要ssl才行
        server = smtplib.SMTP_SSL(email_conf['smtp_server'], email_conf['smtp_port'])
        #server.set_debuglevel(1)
        server.login(email_conf['from_addr'], email_conf['password'])
        server.sendmail(email_conf['from_addr'], email_conf['to_addr'], msg.as_string())
        server.quit()
        return True
    except Exception as e:
        if wlog :
            content = {'msg':msg.items(), 'email_conf' : email_conf}
            log('发送邮件失败:' + str(e), send=False, content=content)
        return False

def upload(src_file, dist_file):
    if not SERVER:
        flag = uploadTencent(src_file, dist_file)
        flag = uploadQiniu(src_file, dist_file)
    else:
        if SERVER == 'tencent':
            flag = uploadTencent(src_file, dist_file)
        else:
            flag = uploadQiniu(src_file, dist_file)
    rmLocalFile(src_file)
    return flag

'''
    删除本地文件
'''
def rmLocalFile(file_name):
    if os.path.exists(file_name) :
        return os.remove(file_name)
    return True

'''
    上传到七牛
'''
def uploadQiniu(src_file, dist_file):
    try:
        bucket_name = CONFIG['qiniu']['bucket']
        ak = CONFIG['qiniu']['access_key']
        sk = CONFIG['qiniu']['secret_key']
        domain = CONFIG['qiniu']['domain']
        token_timeout = 3600
        qiniu = Auth(ak, sk)
        token = qiniu.upload_token(bucket_name, dist_file, token_timeout)
        ret, info = put_file(token, dist_file, src_file)
        if ret == None :
            raise Exception(info)
        #assert ret['key'] == dist_file
        #assert ret['hash'] == etag(src_file)

        days = 30
        url = domain + '/' + dist_file
        down_url = qiniu.private_download_url(url, 86400*days)
        msg = '上传七牛成功:' + json.dumps(ret) + "\r\n"
        msg = msg + "下载链接("+str(days)+"天有效)："+down_url
        log(msg, title='上传七牛成功')

        if days > 0:
            bucket = BucketManager(qiniu)
            bucket.delete_after_days(bucket_name, dist_file, str(days))
        return True
    except Exception as e:
        log('上传七牛失败:' + str(e), title='上传七牛失败')
        return False
'''
    上传到腾讯
'''
def uploadTencent(src_file, dist_file):
    try:
        secret_id = CONFIG['tencent']['secret_id']
        secret_key = CONFIG['tencent']['secret_key']
        region = CONFIG['tencent']['region']
        appid = CONFIG['tencent']['appid']
        bucket = CONFIG['tencent']['bucket']
        token = ''  # 使用临时秘钥需要传入Token，默认为空,可不填
        conf = CosConfig(Appid=appid, Region=region, Secret_id=secret_id, Secret_key=secret_key, Token=token)  # 获取配置对象
        client = CosS3Client(conf)
        # 字节流 简单上传
        with open(src_file, 'rb') as fp:
            response = client.put_object(
                Bucket=bucket,
                Body=fp,
                Key=dist_file,
                CacheControl='no-cache',
                #ContentDisposition='download.txt'
            )
        #下载链接三十天有效
        days = 30
        down_url = client.get_presigned_download_url(bucket, dist_file, 86400*days)
        msg = '上传腾讯成功:' + str(response) + "\r\n"
        msg = msg + "下载链接("+str(days)+"天有效)："+down_url
        log(msg, title='上传腾.讯成功')
        return True
    except Exception as e:
        log('上传腾讯失败:' + str(e), title='上传腾.讯失败')
        return False


if __name__ == '__main__':

    parser = optparse.OptionParser(u"usage %prog [-选项] [--config=my.ini] [--db=my_database] <备份资源(文件)> <备份资源(目录)>")

    parser.add_option('--db', type='string', help=u'备份数据库名，多个请用,分割')
    parser.add_option('--server', type='string', help=u'备份七牛或者腾讯：qiniu,tencent；默认将备份所有有配置信息的云存储')
    parser.add_option('--config', type='string', help=u'配置文件')
    parser.add_option('--days', type='int', help=u'备份时间，过期将会被清除(此功能暂时不稳定)')
    parser.add_option('--log', type='string', help=u'日志文件')

    (options, args) = parser.parse_args()

    if len(args) == 0 and options.db == None:
        print parser.get_usage()
        exit()

    #读取文件配置
    ini = options.config if options.config else DEFAULT_INI

    if not os.path.isfile(ini):
        log('配置文件不存在:' + ini, title='配置文件不存在')
        exit()
    CONFIG = readIni(ini)

    #更新命令参数配置
    #if options.days:
        #CONFIG['qiniu']['days'] = options.days

    if options.log:
        #日志文件
        if os.path.isdir(options.log) :
            if os.access(options.log, os.W_OK):
                LOG_FILE = options.log.strip('/') + SELF + '.log'
        else :
            LOG_FILE = options.log
    if options.server:
        SERVER = options.server

    #备份数据库
    if options.db != None:
        bakDB(options.db)

    #备份文件或者目录
    if len(args) > 0:
        for x in args:
            bakDirOrFile(x)
