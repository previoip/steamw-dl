import os
import sys
import json
import threading
import re
import urllib.parse
import requests.exceptions
from requests import Session
from requests.structures import CaseInsensitiveDict
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from collections import defaultdict
from time import time, sleep
from math import isclose
from argparse import ArgumentParser


class fscache:

  @staticmethod
  def _compile_paths(paths):
    return os.path.join(*map(str, paths))

  @classmethod
  def get(cls, *paths):
    path = cls._compile_paths(paths)
    if not os.path.exists(path) and not os.path.isfile(path):
      return None
    with open(path, 'rb') as fp:
      return fp.read()

  @classmethod
  def set(cls, b, *paths):
    path = cls._compile_paths(paths)
    if os.path.exists(path) and os.path.isfile(path):
      return
    dirpath = os.path.dirname(path)
    if not os.path.exists(dirpath) and not os.path.isdir(dirpath):
      os.makedirs(dirpath)
    with open(path, 'wb') as fp:
      return fp.write(b)


class Timer:

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

  @staticmethod
  def fmt_hms(s):
    m = int(s // 60)
    h = m // 60
    s %= 60
    m %= 60
    s = int(s)
    return f'{h: 3d}:{m:02d}:{s:02d}'


class Speedtally:

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


class bytenum_fmt:

  unit = ['b', 'kb', 'Mb', 'Gb', 'Tb']
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

  def __init__(self):
    self.fields = list()
    self.repls = dict()
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

  @staticmethod
  def barrepr(frac, n, filla='#', fillb='.'):
    n = max(4, n-2)
    frac = max(min(frac, 1), 0)
    nl = int(round(frac*n, 0))
    nr = max(0, n-nl)
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

  def add_entry(self, name, size, repl=None):
    self.fields.append(name)
    self.sizes.append(size)
    self.speeds.append(Speedtally(80))
    self.tally.append(0)
    self.status.append(0)
    self.nents += 1
    if repl:
      self.repls[name] = repl

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

    lapsed_s = ' elapsed'
    lapsed_s += f'{Timer.fmt_hms(self.timer.glapsed())}'
    remain_s = ''
    if remain_size > 0 and total_speed > 0:
      remain_s = ' remaining'
      remain_s += Timer.fmt_hms(remain_size/total_speed)

    sys.stdout.write('\x1b[2K')
    sys.stdout.write(f'processing... ({sum(map(lambda v: v>0, self.status))}/{self.nents}){lapsed_s}{remain_s}\n')

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

      name = self.repls.get(name, name)
      name_s = self.stringroll(name, name_w, self.rollcounter)  + '|'
      rate_s = f'{bytenum_fmt.fmt(rate)}/s'.rjust(rate_w, ' ')
      sums_s = f'({bytenum_fmt.fmt(sums)}) '.rjust(sums_w, ' ')

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

  @staticmethod
  def _http_head(sess: Session, url):
    cache_paths = ['cache', 'head-requests', *(urllib.parse.urlparse(url).path.rstrip('/') + '.json').split('/')]
    cached = fscache.get(*cache_paths)
    if cached:
      return CaseInsensitiveDict(json.loads(cached))
    with sess.head(url, allow_redirects=True) as resp:
      if resp.status_code != 200:
        return {}
      headers = resp.headers
    fscache.set(json.dumps(dict(headers)).encode(), *cache_paths)
    return headers

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

  @staticmethod
  def _iterstream(url: str,
                  session: Session,
                  status_queue: Queue = None,
                  terminate_event: threading.Event = None,
                  trackerid = None,
                  chunk_size: int = 1024**2,
                  requests_http_get_kw = {}):
    queue_ok = not status_queue is None
    event_ok = not terminate_event is None
    try:
      with session.get(url, stream=True, allow_redirects=True, **requests_http_get_kw) as resp:
        if resp.status_code != 200:
          if queue_ok: status_queue.put((trackerid, resp.status_code, 0))
          yield None
          return
        for chunk in resp.iter_content(chunk_size=chunk_size):
          if event_ok and terminate_event.is_set(): break
          if queue_ok: status_queue.put((trackerid, 200, len(chunk)))
          yield chunk
        if queue_ok: status_queue.put((trackerid, 0, 0))
    except requests.exceptions.ChunkedEncodingError:
      if queue_ok: status_queue.put((trackerid, 418, 0))
    ...
    return

  @classmethod
  def _download_worker(cls,
                       entry,
                       session: Session,
                       status_queue: Queue,
                       terminate_event: threading.Event,
                       chunk_size: int = 1024**2,
                       requests_http_get_kw = {}):
    url, filepath, _, trackerid = entry
    if os.path.exists(filepath):
      status_queue.put((trackerid, 0, 0))
      return
    dirname = os.path.dirname(filepath)
    if not os.path.exists(dirname): os.makedirs(dirname)
    with open(filepath, 'wb') as fp:
      for chunk in cls._iterstream(url, session, status_queue, terminate_event,
                                   trackerid, chunk_size, **requests_http_get_kw):
        if chunk: fp.write(chunk)
    return

  @staticmethod
  def _tracker_worker(entries,
                      status_queue: Queue,
                      terminate_event: threading.Event,
                      enable_progress_printing=True):
    pbar = ProgressBar()
    pbar.disable = not enable_progress_printing
    timer = Timer()
    tid_to_key = dict()
    for url, filepath, filesize, trackerid in entries:
      pbar.add_entry(url, filesize, repl=os.path.basename(filepath))
      tid_to_key[trackerid] = url
    n_tasks = len(entries)
    n_tasks_done = 0

    while n_tasks_done < n_tasks:
      if terminate_event.is_set(): return
      while status_queue.qsize() == 0:
        if terminate_event.is_set(): return
        sleep(.01)
      status_item = status_queue.get()
      trackerid, status_code, chunklen = status_item
      url = tid_to_key[trackerid]
      if status_code != 200:
        n_tasks_done += 1
        pbar.set_status_finished(url)
      elif status_code != 0:
        pass
      pbar.update(url, chunklen)
      if not timer.has_lapsed(0, .05): continue
      pbar.render()
    pbar.render()
    terminate_event.set()

  @classmethod
  def _tracker_thread_setup(cls,
                            entries,
                            status_queue: Queue,
                            terminate_event: threading.Event,
                            enable_progress_printing=True):
    return threading.Thread(
      target=cls._tracker_worker,
      args=(entries, status_queue, terminate_event, enable_progress_printing),
      daemon=True
    )

  @classmethod
  def preprocess_entries(cls, sess: Session, entries, path, overwrite=True):
    prepro_entries = list()
    urldupes = list()
    for trackerid, item in enumerate(entries):
      url, filename, filesize = item
      if url in urldupes: continue # found dupe
      urldupes.append(url)

      if not filename or filesize is None:
        headers = cls._http_head(sess, url)
        new_filename = cls._headers_parse_filename(headers)
        filesize = cls._headers_parse_content_length(headers)
        filename = new_filename if new_filename else os.path.basename(url.rstrip('/'))
      filepath = os.path.join(path, filename)

      if not overwrite and os.path.exists(filepath):
        counter = 0
        ckfilepath = filepath
        while os.path.exists(ckfilepath):
          lfilepath, _, ext = filepath.rpartition('.')
          if not lfilepath:
            lfilepath = ext
            ext = ''
          if ext: ext = '.'+ext
          ckfilepath = f'{lfilepath}_{counter}{ext}'
          counter += 1
        filepath = ckfilepath
      prepro_entries.append((url, filepath, filesize, trackerid))
    return prepro_entries

  @classmethod
  def download_multiple(cls, session: Session, entries, max_workers=8, enable_progress_printing=True):
    status_queue = Queue(1024)
    terminate_event = threading.Event()
    tracker_daemon = cls._tracker_thread_setup(entries, status_queue, terminate_event, enable_progress_printing)
    tracker_daemon.start()
    with ThreadPoolExecutor(max_workers=max_workers) as tp:
      for entry in entries:
        tp.submit(cls._download_worker, entry, session, status_queue, terminate_event)
      try:
        while not terminate_event.is_set(): sleep(.1)
      except KeyboardInterrupt:
        terminate_event.set()
        print('KeyboardInterrupt: waiting for threads to shutdown')
    tracker_daemon.join()

  @classmethod
  def download(cls, session: Session, entry, enable_progress_printing=True):
    cls.download_multiple(session, [entry], max_workers=1, enable_progress_printing=enable_progress_printing)


class swapi:

  @staticmethod
  def _post_swapi(sess: Session, interface, method, version, data):
    cache_paths = [os.path.dirname(__file__), 'cache', 'swapi-requests', interface, method, version]
    key = data.get('publishedfileids[0]')
    key = key if key else data.get('publishedfileid')

    cached = fscache.get(*cache_paths, key)
    if cached: return cached

    resp = sess.post(f'https://api.steampowered.com/{interface}/{method}/{version}/', data=data)
    resp.raise_for_status()
    content = resp.content

    if content: fscache.set(content, *cache_paths, key)
    return content

  @classmethod
  def get_collection_details_v1(cls, sess: Session, publishedfileid: int):
    interf = 'ISteamRemoteStorage'
    method = 'GetCollectionDetails'
    version = 'v1'
    content = cls._post_swapi(sess, interf, method, version,
                              {'collectioncount':1, 'publishedfileids[0]': publishedfileid})
    return json.loads(content)

  @classmethod
  def get_published_file_details_v1(cls, sess: Session, publishedfileid: int):
    interf = 'ISteamRemoteStorage'
    method = 'GetPublishedFileDetails'
    version = 'v1'
    content = cls._post_swapi(sess, interf, method, version,
                              {'itemcount':1, 'publishedfileids[0]': publishedfileid})
    return json.loads(content)


class steamw_dl:

  _traversed = list()

  @classmethod
  def get_workshop_info(cls, sess: Session, workshop_id):
    collection_info = swapi.get_collection_details_v1(sess, workshop_id)
    collectiondetails = collection_info['response']['collectiondetails']
    for collection in collectiondetails:
      if 'children' in collection:
        for collectionchild in collection['children']:
          child_workshop_id = int(collectionchild['publishedfileid'])
          if child_workshop_id in cls._traversed: continue
          cls._traversed.append(child_workshop_id)
          yield from cls.get_workshop_info(sess, child_workshop_id)
      else:
        workshop_id = int(collection['publishedfileid'])
        workshop_info = swapi.get_published_file_details_v1(sess, workshop_id)
        yield from workshop_info['response'].get('publishedfiledetails', [])

  @classmethod
  def fetch_workshop_id_urls(cls, sess: Session, workshop_ids):
    entries = list()
    counter = 1
    for workshop_id in workshop_ids:
      for workshop_info in cls.get_workshop_info(sess, workshop_id):
        fileid = workshop_info['publishedfileid']
        appid = workshop_info['consumer_app_id']
        title = workshop_info['title']  
        filename = workshop_info.get('filename',  '')
        filesize = int(workshop_info.get('file_size',  0))  
        fileurl = workshop_info.get('file_url',  '')
        previewurl = workshop_info.get('preview_url',  '')
        if previewurl: entries.append((previewurl, None, None))
        if filename:
          filename = os.path.basename(filename)
        print()
        if fileurl:
          print(f'({counter: 3d}) found {appid} {fileid}: "{title}"')
          print(f'      file  : ({bytenum_fmt.fmt(filesize)}) {filename}')
          print(f'      url   : {fileurl}')
          print(f'      thumb : {previewurl}')
          entries.append((fileurl, filename, filesize))
          counter += 1
        else:
          print(f'  !   missing url appid : {appid} wid:{fileid}: {title}')
          print(f'      file  : {filename if filename else "unknown"}')
    print()
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

  sess = Session()

  entries = list()
  entries.extend(steamw_dl.fetch_workshop_id_urls(sess, args.workshopids))
  if len(entries) == 0:
    print('no files to download')
    exit()

  entries = downloader.preprocess_entries(sess, entries, args.path, overwrite=True)

  totalsize = 0
  for _, _, filesize, _ in entries:
    if isinstance(filesize, int):
      totalsize += filesize

  print()
  print(f'total download size : {bytenum_fmt.fmt(totalsize)}')
  print(f'files to download   : {len(entries)}')
  print(f'download location   : {args.path}')
  if input('continue? [Y/n] ') != 'Y': exit()

  if not os.path.exists(args.path):
    os.makedirs(args.path)

  max_workers = min(max(1, args.max_workers), 16)
  downloader.download_multiple(sess, entries, max_workers, enable_progress_printing=True)
