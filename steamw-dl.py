import os
import sys
import json
import requests
import threading
import re
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from io import IOBase
from queue import Queue
from collections import namedtuple, defaultdict
from time import time, sleep
from math import isclose
from argparse import ArgumentParser



class Timer:

  @staticmethod
  def fmt_hms(s):
    m = int(s // 60)
    h = m // 60
    s %= 60
    m %= 60
    s = int(s)
    return f'{h: 3d}:{m:02d}:{s:02d}'

  def __init__(self):
    self.t0 = time()
    self.ckpt = defaultdict(time)

  def glapsed(self):
    return time() - self.t0

  def has_lapsed(self, key, sec):
    t = time()
    s = t - self.ckpt[key]
    if s > sec:
      self.ckpt[key] = t
      return True
    return False


class Speed:
  def __init__(self, n=10):
    self.n = n
    self.t0 = time()
    self.cursor = 0
    self.deltas = [None for _ in range(n)]
    self.values = [None for _ in range(n)]

  def reset(self):
    self.cursor = 0
    for i in range(self.n):  
      self.deltas[i] = None
      self.values[i] = None


  def update(self, value):
    t = time()
    self.deltas[self.cursor] = t - self.t0
    self.values[self.cursor] = value
    self.t0 = t
    self.cursor += 1
    self.cursor %= self.n

  def get(self):
    sums = 0
    count = self.n
    for i in range(self.n):
      dt = self.deltas[i]
      dv = self.values[i]
      if dt is None or dv is None or isclose(dt, 0):
        count -= 1
        continue
      sums += dv/dt
    if count == 0:
      return 0
    return sums / count


class BytenumFmt:
  unit = ['b', 'kb', 'mb', 'gb', 'Tb']
  logs = [1024**i for i in range(len(unit))]
  @classmethod
  def fmt(cls, n):
    for i, l in enumerate(cls.logs):
      if n-l<0:
        i = max(i-1, 0)
        d = cls.logs[i]
        u = cls.unit[i]
        return f'{n/d:.01f}{u}' 


class ProgressBar:

  @staticmethod
  def barrepr(frac, n, filla='#', fillb='.'):
    n = max(4, n-2)
    frac = max(min(frac, 1), 0)
    nl = int(round(frac*n, 0))
    nr = max(0, n-nl)
    # percent = f' {frac: 4.0%}'
    return '[' + filla*nl + fillb*nr + ']'

  @staticmethod
  def stringroll(s, width, offset):
    length = len(s)
    if length <= width:
      return s.ljust(width, '.')
    s += '  '
    length += 2
    offset %= width*2
    roff = offset+width
    l = s[offset:roff]
    r = ''
    if roff > length:
      r = s[:roff%length]
    return (l + r)[:width]

  def __init__(self):
    self.fields = list()
    self.sizes = list()
    self.speeds = list()
    self.tally = list()
    self.timer = Timer()
    self.nents = 0
    self.status = list()
    self.rendercounter = 0
    self.finishedcounter = 0
    self.flushing = False
    self.rollcounter = 0
    self.disable = False

  def add_entry(self, name, size):
    self.fields.append(name)
    self.sizes.append(size)
    self.speeds.append(Speed(80))
    self.tally.append(0)
    self.status.append(0)
    self.nents += 1

  def update(self, name, incr):
    i = self.fields.index(name)
    self.tally[i] += incr
    self.speeds[i].update(incr)
    if self.tally[i] == self.sizes[i]:
      self.set_status_finished(name)


  def set_status_finished(self, name):
    i = self.fields.index(name)
    self.status[i] = 1
    self.speeds[i].reset()

  def render(self):
    if self.disable: return
    nline = self.nents+1
    if self.rendercounter == 0:
      for _ in range(nline): print()
    if self.timer.has_lapsed(0, .5):
      self.rollcounter += 1
    sys.stdout.write(f'\x1b[{nline}A')
    self.rendercounter += 1

    target_size = sum(self.sizes)
    remain_size = target_size - sum(self.tally)
    total_speed = sum(filter(lambda v: v and v>0, map(lambda i: i.get(), self.speeds)))
    remain_s = ''
    if remain_size > 0 and total_speed > 0:
      remain_s = ' remaining'
      remain_s += Timer.fmt_hms(remain_size/total_speed)

    sys.stdout.write('\x1b[2K')
    sys.stdout.write(f'processing... ({sum(self.status)}/{self.nents}) elapsed{Timer.fmt_hms(self.timer.glapsed())}' + remain_s + '\n')

    disp_w = 56
    name_w = 14
    perc_w = 5
    rate_w = 10
    sums_w = 11
    pbar_w = disp_w - name_w - perc_w - rate_w - sums_w

    for i in range(self.nents):
      # stat = self.status[i]
      name = self.fields[i]
      size = self.sizes[i]
      sums = self.tally[i]
      rate = self.speeds[i].get()

      name_s = self.stringroll(name, name_w, self.rollcounter)  + '|'
      rate_s = f'{BytenumFmt.fmt(rate)}/s'.rjust(rate_w, ' ')
      sums_s = f'({BytenumFmt.fmt(sums)}) '.rjust(sums_w, ' ')

      if size == 0:
        pbar_s = ''
        perc_s = ''
      else:
        frac = sums/size
        pbar_s = self.barrepr(frac, pbar_w)
        perc_s = f'{frac: 4.0%}'.rjust(perc_w, ' ')

      s = name_s + rate_s + sums_s + pbar_s + perc_s
      sys.stdout.write('\x1b[2K')
      sys.stdout.write(s + '\n')


class downloader:
  _msgq = Queue(maxsize=1024)
  _event_terminate = threading.Event()
  _nt_fileinfo = namedtuple('FileInfo', ['filename', 'filesize'])

  class status:
    onprogress = 2
    failed = 1
    success = 0

  @staticmethod
  def _http_head(sess: requests.Session, url):
    with sess.head(url, allow_redirects=True) as resp:
      if resp.status_code != 200:
        return {}
      return resp.headers

  @staticmethod
  def _headers_parse_content_length(headers):
    return int(headers.get("Content-Length", 0))

  @staticmethod
  def _headers_parse_filename(headers):
    # stackoverflow.com/questions/37060344/how-to-determine-the-filename-of-content-downloaded-with-http-in-python
    info = headers.get("Content-Disposition")
    if info is None:
      return None
    fname = re.findall(r"filename\*=([^;]+)", info, flags=re.IGNORECASE)
    if not fname:
      fname = re.findall(r"filename=([^;]+)", info, flags=re.IGNORECASE)
    if "utf-8''" in fname[0].lower():
      fname = re.sub("utf-8''", '', fname[0], flags=re.IGNORECASE)
      fname = urllib.parse.unquote(fname)
    else:
      fname = fname[0]
    return fname.strip().strip('"')

  @classmethod
  def _http_head_parse_fileinfo(cls, sess, url):
    headers = cls._http_head(sess, url)
    filesize = cls._headers_parse_content_length(headers)
    filename = cls._headers_parse_filename(headers)
    if filename is None:
      filename = url.rstrip('/').rpartition('/')[-1]
    return cls._nt_fileinfo(filename, filesize)

  @classmethod
  def _streamio(cls, sess: requests.Session, bufp: IOBase, url, trackerid=None, chunk_size=2**18):
    sums = 0
    with sess.get(url, stream=True, allow_redirects=True) as resp:
      if resp.status_code != 200:
        cls._msgq.put((trackerid, cls.status.failed, (0, 0, f'{resp.status_code}')))
        return
      try:
        for chunk in resp.iter_content(chunk_size=chunk_size):
          if cls._event_terminate.is_set():
            cls._msgq.put((trackerid, cls.status.failed, (0, sums, f'terminated')))
            break
          if chunk:
            bufp.write(chunk)
          chunksize = len(chunk)
          sums += chunksize
          cls._msgq.put((trackerid, cls.status.onprogress, (chunksize, sums, '')))
        cls._msgq.put((trackerid, cls.status.success, (0, sums, 'success')))
        return
      except requests.exceptions.ChunkedEncodingError:
        cls._msgq.put((trackerid, cls.status.failed, (0, sums, f'{resp.status_code}')))
        return

  @classmethod
  def _streamfp(cls, sess: requests.Session, path, url, trackerid=None):
    with open(path, 'wb') as fp:
      cls._streamio(sess, fp, url, trackerid)

  @classmethod
  def _progress_renderer_worker(cls, batch):
    entries = list(batch.keys())
    n_tasks = len(entries)
    n_task_done = 0
    timer = Timer()
    pbar = ProgressBar()

    for trackerid in entries:
      pbar.add_entry(batch[trackerid]['filename'], batch[trackerid]['filesize'])

    timeoutc = 0 
    while n_task_done < n_tasks:
      if cls._event_terminate.is_set():
        break

      if cls._msgq.qsize() == 0:
        sleep(.25)
        pbar.render()
        timeoutc += 1
        if timeoutc > 120 * 4:
          break
        continue

      timeoutc = 0
      qitem = cls._msgq.get()
      trackerid, status, progressinfo = qitem
      if status in (cls.status.failed, cls.status.success):
        n_task_done += 1
        pbar.set_status_finished(batch[trackerid]['filename'])
      chunksize, _, _ = progressinfo
      pbar.update(batch[trackerid]['filename'], chunksize)
      if not timer.has_lapsed(0, .05): continue
      pbar.render()
    pbar.render()


  @classmethod
  def _progress_renderer_getdaemon(cls, batch):
    return threading.Thread(
      target=cls._progress_renderer_worker,
      args=(batch,),
      daemon=True
    )

  @classmethod
  def _execute(cls, batch, max_workers=4):
    renderer_daemon = cls._progress_renderer_getdaemon(batch)
    renderer_daemon.start()
    with ThreadPoolExecutor(max_workers=max_workers) as tpexec:
      for trackerid in batch:
        entry = batch[trackerid]
        tpexec.submit(cls._streamfp, entry['sess'], entry['filepath'], entry['url'], trackerid)
      try:
        while True: sleep(.1)
      except KeyboardInterrupt:
        cls._event_terminate.set()
        print('KeyboardInterrupt: waiting for thread to shutdown')
    renderer_daemon.join()

  @classmethod
  def download(cls, sess: requests.Session, path, url, filename=None, filesize=None):
    batch = dict()
    if not filename or filesize is None:
      fileinfo = cls._http_head_parse_fileinfo(sess, url)
      filename = filename if filename else fileinfo.filename
      filesize = filesize if filesize else fileinfo.filesize
    filepath = os.path.join(path, filename)      
    batch[0] = {'sess':sess, 'url':url, 'filename': filename, 'filesize': filesize, 'filepath': filepath}
    cls._execute(batch, 1)


  @classmethod
  def download_batch(cls, sess, path, entries, max_workers=8):
    batch = dict()
    for i, (url, filename, filesize) in enumerate(entries):
      if not filename or filesize is None:
        fileinfo = cls._http_head_parse_fileinfo(sess, url)
        filename = filename if filename else fileinfo.filename
        filesize = filesize if filesize else fileinfo.filesize
      filepath = os.path.join(path, filename)
      batch[i] = {'sess':sess, 'url':url, 'filename': filename, 'filesize': filesize, 'filepath': filepath}
    cls._execute(batch, max_workers)


class swapi:

  @staticmethod
  def _post(sess: requests.Session, interface, method, version, data):
    cache_path = os.path.join(os.path.dirname(__file__), 'cache', 'swapi-requests', interface, method, version)
    if not os.path.exists(cache_path):
      os.makedirs(cache_path)
    key = data.get('publishedfileids[0]')
    key = key if key else data.get('publishedfileid')
    cache_filepath = os.path.join(cache_path, str(key))
    if key and os.path.exists(cache_filepath):
      with open(cache_filepath, 'rb') as fp:
        return fp.read()

    resp = sess.post(
      f'https://api.steampowered.com/{interface}/{method}/{version}/', data=data
    )
    resp.raise_for_status()
    content = resp.content

    if key and not os.path.exists(cache_filepath):
      with open(cache_filepath, 'wb') as fp:
        fp.write(content)

    return content

  @classmethod
  def get_collection_details_v1(cls, sess: requests.Session, publishedfileid: int):
    interf = 'ISteamRemoteStorage'
    method = 'GetCollectionDetails'
    version = 'v1'
    content = cls._post(sess, interf, method, version,
                        {'collectioncount':1, 'publishedfileids[0]': publishedfileid})
    return json.loads(content)

  @classmethod
  def get_published_file_details_v1(cls, sess: requests.Session, publishedfileid: int):
    interf = 'ISteamRemoteStorage'
    method = 'GetPublishedFileDetails'
    version = 'v1'
    content = cls._post(sess, interf, method, version,
                        {'itemcount':1, 'publishedfileids[0]': publishedfileid})
    return json.loads(content)


class steamw_dl:

  @classmethod
  def get_workshop_info(cls, sess, workshop_id):
    collection_info = swapi.get_collection_details_v1(sess, workshop_id)
    collectiondetails = collection_info['response']['collectiondetails']
    for collection in collectiondetails:
      workshop_id = int(collection['publishedfileid'])
      if 'children' in collection:
        for collectionchild in collection['children']:
          child_workshop_id = int(collectionchild['publishedfileid'])
          yield from cls.get_workshop_info(sess, child_workshop_id)
      else:
        workshop_info = swapi.get_published_file_details_v1(sess, workshop_id)
        yield from workshop_info['response'].get('publishedfiledetails', [])

  @classmethod
  def fetch_workshop_id_urls(cls, sess: requests.Session, workshop_ids):
    entries = list()
    for workshop_id in workshop_ids:
      for workshop_info in cls.get_workshop_info(sess, workshop_id):
        fileid = workshop_info['publishedfileid']
        appid = workshop_info['consumer_app_id']
        title = workshop_info['title']  
        filename = workshop_info.get('filename',  '')
        filesize = int(workshop_info.get('file_size',  0))  
        fileurl = workshop_info.get('file_url',  '')
        # previewurl = workshop_info.get('preview_url',  '')
        # if previewurl: urls.append((previewurl, None, None))
        if filename:
          filename = os.path.basename(filename)
        print()
        if fileurl:
          print(f'- found {appid} {fileid}: "{title}"')
          print(f'  file : {filename} ({BytenumFmt.fmt(filesize)})')
          print(f'  url  : {fileurl}')
          entries.append((fileurl, filename, filesize))
        else:
          print(f'! missing url appid:{appid} wid:{fileid}: {title}')
          print(f'  file: {filename if filename else "unknown"}')
          continue
    return entries


if __name__ == '__main__':

  parser = ArgumentParser('steamw-dl')
  parser.add_argument('path', type=str)
  parser.add_argument('workshopids', type=int, nargs='*')
  parser.add_argument('--max-workers', type=int, default=4)

  args = parser.parse_args()
  if len(args.workshopids) == 0:
    parser.print_help()
    exit()

  sess = requests.Session()
  entries = list()
  entries.extend(steamw_dl.fetch_workshop_id_urls(sess, args.workshopids))

  if len(entries) == 0:
    print('no files to download')
    exit()

  totalsize = 0
  for _, _, filesize in entries:
    totalsize += filesize

  print()
  print(f'total download size : {BytenumFmt.fmt(totalsize)}')
  print(f'files to download   : {len(entries)}')
  print(f'download location   : {args.path}')
  if input('continue? [Y/n] ') != 'Y': exit()

  max_workers = min(max(1, args.max_workers), 8)

  if not os.path.exists(args.path):
    os.makedirs(args.path)
  downloader.download_batch(sess, args.path, entries, max_workers)
