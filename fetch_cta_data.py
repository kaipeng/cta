from StringIO import StringIO
from zipfile import ZipFile
import urllib2, sys, os
import datetime

def chunk_report(bytes_so_far, chunk_size, total_size):
    percent = float(bytes_so_far) / total_size
    percent = round(percent*100, 2)
    sys.stdout.write("Downloaded %d of %d bytes (%0.2f%%)\r" %
                     (bytes_so_far, total_size, percent))

    if bytes_so_far >= total_size:
        sys.stdout.write('\n')

def chunk_read(response, chunk_size=8192*2, report_hook=None):
    total_size = response.info().getheader('Content-Length').strip()
    total_size = int(total_size)
    bytes_so_far = 0
    data = ''

    while 1:
        chunk = response.read(chunk_size)
        data += (chunk)
        bytes_so_far += len(chunk)

        if not chunk:
            break

        if report_hook:
            report_hook(bytes_so_far, chunk_size, total_size)

    return data

def simple_download(response):
    zipfile = ZipFile(StringIO(response.read()))
    zipfile.extractall('google_transit/')

def chunk_download(response, dl_path):
    data = chunk_read(response, report_hook=chunk_report)

    f = open(dl_path, 'w+')
    f.write(data)
    f.close()
    return data

def unzip(path):
    zipfile = ZipFile(open(path))
    zipfile.extractall(path.split('.')[0])

if __name__ == '__main__':
    #TODO: check website for last-update info. if new version is available, then download

    data_root_url = "http://www.transitchicago.com/downloads/sch_data/"
    zip_path = 'google_transit.zip'
    data_url = data_root_url + zip_path

    need_update = True
    try:
        response = urllib2.urlopen(data_url)
        cta_last_update = response.info().getheader('Last-Modified').strip()
        cta_last_update_dt = datetime.datetime.strptime(cta_last_update, '%a, %d %b %Y %H:%M:%S %Z')
        print "CTA last update: ", cta_last_update_dt

        statbuf = os.stat(zip_path)
        local_last_update_dt = datetime.datetime.fromtimestamp(int(statbuf.st_mtime))
        print "Local last modified: ", local_last_update_dt

        need_update = cta_last_update_dt > local_last_update_dt
    except Exception as e:
        print e

    if need_update:
        print "Downloading zipped data from: ", data_url
        response = urllib2.urlopen(data_url)
        chunk_download(response, zip_path)

        print "Download complete. Unzipping from: ", zip_path
        unzip(zip_path)
    else:
        print "Local file up to date."


