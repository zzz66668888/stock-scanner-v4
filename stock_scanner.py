#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票技术形态自动扫描系统 v5.2
数据源：tushare pro(主) + baostock(备) + 东方财富港股通
全量扫描A股全部股票 + 港股通标的
形态：底背离 / 上升趋势(左侧交易) / 首板 / 连板 / 顶背离 / 底部即将启动
每只匹配股票附带分析说明
"""

import json, time, sys, traceback, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import baostock as bs

# Tushare Pro - 优先使用购买的token
import os
TUSHARE_TOKENS = [
    os.environ.get('TUSHARE_TOKEN', 'ppqq5e5c1eb3bdf1c92d33fb58abbd123ef0dc25441b2b4ac06c51c1'),  # 环境变量或默认值
    '5ef8653d988f06566b63d4fd869a9e227b9c4c4dad399a4d1315a6c5',  # 原始token（备用）
]

tushare_pro = None
TUSHARE_AVAILABLE = False

def init_tushare():
    """初始化tushare，尝试多个token"""
    global tushare_pro, TUSHARE_AVAILABLE
    try:
        import tushare as ts
        for i, token in enumerate(TUSHARE_TOKENS):
            try:
                ts.set_token(token)
                pro = ts.pro_api()
                # 测试token
                df = pro.stock_basic(exchange='', list_status='L', fields='ts_code', limit=1)
                if df is not None and len(df) > 0:
                    tushare_pro = pro
                    TUSHARE_AVAILABLE = True
                    print(f"[OK] Tushare Pro 初始化成功 (Token {i+1})")
                    return True
            except Exception as e:
                print(f"[WARN] Token {i+1} 失败: {e}")
                continue
        print("[WARN] 所有Token均不可用")
        return False
    except Exception as e:
        print(f"[WARN] Tushare 初始化异常: {e}")
        return False

init_tushare()

from flask import Flask, request, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# ========== 日志 ==========
LOG = []
SCAN_STATUS = {'running': False, 'progress': 0, 'total': 0, 'matched': 0}

def log(msg):
    s = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
    LOG.append(s)
    print(s)
    if len(LOG) > 500: LOG.pop(0)

# ========== 缓存 ==========
_cache = {}
_cache_lock = threading.Lock()

def get_cache(k):
    with _cache_lock:
        if k in _cache:
            d, t = _cache[k]
            if time.time() - t < 300: return d  # 5分钟缓存，兼顾实时性
    return None

def set_cache(k, d):
    with _cache_lock: _cache[k] = (d, time.time())

# ========== 数据获取 ==========

_bs_logged_in = False
_bs_fail_count = 0

def ensure_baostock():
    """确保baostock已登录，失败自动重连"""
    global _bs_logged_in, _bs_fail_count
    if _bs_logged_in and _bs_fail_count < 5:
        return True
    try:
        bs.login()
        _bs_logged_in = True
        _bs_fail_count = 0
        return True
    except:
        _bs_fail_count += 1
        _bs_logged_in = False
        return False

def init_baostock():
    return ensure_baostock()

def fetch_a_stock_list():
    """获取全部A股列表"""
    cached = get_cache('a_list')
    if cached: return cached

    # 优先使用tushare
    if TUSHARE_AVAILABLE:
        try:
            log("正在通过Tushare获取A股列表...")
            df = tushare_pro.stock_basic(exchange='', list_status='L',
                                         fields='ts_code,symbol,name,area,industry,list_date')
            all_stocks = []
            for _, row in df.iterrows():
                code = row['symbol']
                # 排除北交所(8/4/9), B股(2)
                if code.startswith(('8','4','9','2')): continue
                all_stocks.append({
                    'code': code, 'name': row['name'],
                    'full_code': row['ts_code'],
                    'market': 'SH' if code.startswith('6') else 'SZ',
                })
            log(f"A股列表(Tushare): {len(all_stocks)}只")
            set_cache('a_list', all_stocks)
            return all_stocks
        except Exception as e:
            log(f"Tushare获取失败，降级到baostock: {e}")

    # 降级到baostock
    log("正在通过baostock获取A股列表...")
    try:
        rs = bs.query_stock_basic()
        if rs.error_code != '0':
            log(f"获取失败: {rs.error_msg}")
            return []

        all_stocks = []
        while rs.next():
            row = rs.get_row_data()
            code = row[0]; name = row[1]; stock_type = row[4]; status = row[5]
            if status != '1' or stock_type != '1': continue
            short = code.split('.')[-1]
            if short.startswith(('8','4','9','2')): continue
            all_stocks.append({
                'code': short, 'name': name,
                'full_code': code,
                'market': 'SH' if code.startswith('sh') else 'SZ',
            })

        log(f"A股列表(baostock): {len(all_stocks)}只")
        set_cache('a_list', all_stocks)
        return all_stocks
    except Exception as e:
        log(f"获取A股列表异常: {e}")
        return []

def fetch_hk_connect_list():
    """获取港股通标的列表（沪港通+深港通可交易港股）"""
    cached = get_cache('hk_connect')
    if cached: return cached

    log("正在获取港股通标的...")
    result = []

    # 尝试从东方财富获取港股通列表（小请求，不容易被限）
    try:
        import requests
        s = requests.Session()
        s.trust_env = False

        for fs_code, label in [('b:MK0204', '全部港股通'), ('b:MK0205', '沪港通'), ('b:MK0206', '深港通')]:
            try:
                resp = s.get('https://push2.eastmoney.com/api/qt/clist/get', params={
                    'pn': '1', 'pz': '1000', 'po': '1', 'np': '1',
                    'fltt': '2', 'invt': '2', 'fid': 'f3', 'fs': fs_code,
                    'fields': 'f2,f3,f12,f14,f5,f6,f8',
                }, timeout=15, proxies={'http':None,'https':None})

                if resp.status_code == 200:
                    data = resp.json()
                    if data and data.get('data') and data['data'].get('diff'):
                        for item in data['data']['diff']:
                            code = str(item.get('f12',''))
                            name = str(item.get('f14',''))
                            result.append({
                                'code': code, 'name': name,
                                'price': float(item.get('f2',0) or 0),
                                'change_pct': float(item.get('f3',0) or 0),
                                'amount': float(item.get('f6',0) or 0),
                            })
                        log(f"  {label}: {len(data['data']['diff'])}只")
            except:
                pass
            time.sleep(0.3)
    except:
        pass

    # 如果API获取失败，使用常用港股通列表
    if not result:
        log("API获取港股通失败，使用内置列表")
        popular = [
            ('00700','腾讯控股'),('09988','阿里巴巴'),('09999','网易'),('01810','小米集团'),
            ('09618','京东集团'),('09888','百度集团'),('00388','香港交易所'),('02318','中国平安'),
            ('03968','招商银行'),('01299','友邦保险'),('00005','汇丰控股'),('01398','工商银行'),
            ('00939','建设银行'),('03988','中国银行'),('01288','农业银行'),('00883','中国海洋石油'),
            ('00857','中国石油股份'),('02628','中国人寿'),('02333','长城汽车'),('02015','理想汽车'),
            ('09868','小鹏汽车'),('02269','药明生物'),('01093','石药集团'),('02020','安踏体育'),
            ('00175','吉利汽车'),('00027','银河娱乐'),('00016','新鸿基地产'),('00011','恒生银行'),
            ('02007','碧桂园服务'),('01024','快手'),('01833','平安好医生'),('01347','华虹半导体'),
            ('00981','中芯国际'),('02382','舜宇光学'),('01818','招金矿业'),('09633','农夫山泉'),
            ('06160','百济神州'),('09626','哔哩哔哩'),('09899','云音乐'),('06618','京东健康'),
            ('06969','思摩尔国际'),('02013','微盟集团'),('01876','百威亚太'),('01109','华润置地'),
        ]
        for code, name in popular:
            result.append({'code': code, 'name': name, 'price': 0, 'change_pct': 0, 'amount': 0})

    # 去重
    seen = set()
    unique = []
    for r in result:
        if r['code'] not in seen:
            seen.add(r['code'])
            unique.append(r)

    log(f"港股通标的: {len(unique)}只")
    set_cache('hk_connect', unique)
    return unique

def fetch_stock_history(code, market='A', days=120):
    ck = f'h_{market}_{code}_{days}'
    cached = get_cache(ck)
    if cached: return cached

    try:
        if market == 'A':
            # 优先使用tushare
            if TUSHARE_AVAILABLE:
                try:
                    ts_code = f'{code}.SH' if code.startswith('6') else f'{code}.SZ'
                    end_date = datetime.now().strftime('%Y%m%d')
                    start_date = (datetime.now() - timedelta(days=days+30)).strftime('%Y%m%d')
                    df = tushare_pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date,
                                           fields='trade_date,open,high,low,close,vol,amount')
                    if df is not None and len(df) >= 20:
                        df = df.sort_values('trade_date')  # tushare返回倒序，需排序
                        result = {
                            'dates': df['trade_date'].tolist(),
                            'open': df['open'].astype(float).tolist(),
                            'close': df['close'].astype(float).tolist(),
                            'high': df['high'].astype(float).tolist(),
                            'low': df['low'].astype(float).tolist(),
                            'volume': df['vol'].astype(float).tolist(),
                            'turnover': [0]*len(df),  # tushare日线无换手率，后续可补充
                        }
                        set_cache(ck, result)
                        return result
                except Exception as e:
                    log(f"Tushare K线获取失败，降级到baostock: {e}")

            # 降级到baostock
            bs_code = f'sh.{code}' if code.startswith('6') else f'sz.{code}'
            end = datetime.now().strftime('%Y-%m-%d')
            start = (datetime.now() - timedelta(days=days+30)).strftime('%Y-%m-%d')
            rs = bs.query_history_k_data_plus(bs_code,
                'date,open,close,high,low,volume,amount,turn,pctChg',
                start_date=start, end_date=end, frequency='d', adjustflag='2')
            if rs.error_code != '0':
                ensure_baostock()
                return None
            data = rs.get_data()
            if data is None or len(data) < 20: return None
            result = {
                'dates': data['date'].tolist(),
                'open': [float(x) for x in data['open']],
                'close': [float(x) for x in data['close']],
                'high': [float(x) for x in data['high']],
                'low': [float(x) for x in data['low']],
                'volume': [float(x) for x in data['volume']],
                'turnover': [float(x) for x in (data['turn'].tolist() if 'turn' in data.columns else [0]*len(data))],
            }
        else:
            # 港股: 东方财富API
            result = _fetch_hk_history(code, days)
            if result is None: return None

        if len(result['close']) < 20: return None
        set_cache(ck, result)
        return result
    except Exception as e:
        return None

def _fetch_hk_history(code, days=120):
    try:
        import requests
        s = requests.Session(); s.trust_env = False
        end = datetime.now().strftime('%Y%m%d')
        beg = (datetime.now() - timedelta(days=days+30)).strftime('%Y%m%d')
        resp = s.get('https://push2his.eastmoney.com/api/qt/stock/kline/get', params={
            'secid': f'116.{code}', 'fields1': 'f1,f2,f3,f4,f5,f6',
            'fields2': 'f51,f52,f53,f54,f55,f56',
            'klt': '101', 'fqt': '1', 'beg': beg, 'end': end, 'lmt': str(days+30),
        }, timeout=15, proxies={'http':None,'https':None})
        if resp.status_code != 200: return None
        data = resp.json()
        if not data.get('data') or not data['data'].get('klines'): return None
        result = {'dates':[],'open':[],'close':[],'high':[],'low':[],'volume':[]}
        for line in data['data']['klines']:
            p = line.split(',')
            if len(p) >= 6:
                result['dates'].append(p[0]); result['open'].append(float(p[1]))
                result['close'].append(float(p[2])); result['high'].append(float(p[3]))
                result['low'].append(float(p[4])); result['volume'].append(float(p[5]))
        return result if len(result['close']) >= 20 else None
    except:
        return None

# ========== 技术指标 ==========

def ema(s,n): return s.ewm(span=n,adjust=False).mean()
def ma(s,n): return s.rolling(n).mean()
def macd(c,f=12,s=26,sig=9):
    d=ema(c,f)-ema(c,s); e=ema(d,sig); return d,e,2*(d-e)
def rsi(c,n=14):
    d=c.diff(); g=d.clip(lower=0); l=(-d).clip(lower=0)
    return (100-100/(1+(g.ewm(alpha=1/n,adjust=False).mean()/l.ewm(alpha=1/n,adjust=False).mean().replace(0,np.nan)))).fillna(50)
def kdj(h,l,c,n=9,m1=3,m2=3):
    ll=l.rolling(n).min(); hh=h.rolling(n).max()
    rsv=((c-ll)/(hh-ll).replace(0,np.nan))*100; rsv=rsv.fillna(50)
    k=rsv.ewm(alpha=1/m1,adjust=False).mean(); d=k.ewm(alpha=1/m2,adjust=False).mean()
    return k,d,3*k-2*d
def boll(c,n=20,std=2):
    m=ma(c,n); s=c.rolling(n).std(); return m+std*s,m,m-std*s,(m+std*s-(m-std*s))/m*100

def find_lows(a,o=6):
    out=[]
    for i in range(o,len(a)-o):
        if all(a[i]<=a[i-j] for j in range(1,o+1)) and all(a[i]<=a[i+j] for j in range(1,o+1)): out.append(i)
    return np.array(out)
def find_highs(a,o=6):
    out=[]
    for i in range(o,len(a)-o):
        if all(a[i]>=a[i-j] for j in range(1,o+1)) and all(a[i]>=a[i+j] for j in range(1,o+1)): out.append(i)
    return np.array(out)

# ========== 形态检测(含分析说明) ==========

def detect_bottom_divergence(df):
    """
    底背离: 价格创新低，MACD DIF未创新低 → 看涨反转信号
    """
    if len(df) < 40: return False,0,''
    c=df['close'].tail(60); dif,_,_=macd(c); o=6
    pl=find_lows(c.values,o); dl=find_lows(dif.values,o)
    if len(pl)<2 or len(dl)<2: return False,0,''
    p1,p2=pl[-2],pl[-1]
    if c.iloc[p2]>=c.iloc[p1]: return False,0,''
    rdl=[i for i in dl if abs(i-p1)<o or abs(i-p2)<o]
    if len(rdl)<2: return False,0,''
    d1,d2=rdl[-2],rdl[-1]
    if dif.iloc[d2]>dif.iloc[d1]:
        s=round(min(100,abs(dif.iloc[d2]-dif.iloc[d1])/max(abs(dif.iloc[d1]),0.0001)*100+50),1)
        # 生成分析说明
        pct_p=round((c.iloc[p2]-c.iloc[p1])/c.iloc[p1]*100,2)
        pct_d=round((dif.iloc[d2]-dif.iloc[d1])/abs(dif.iloc[d1])*100,2)
        r=rsi(c).iloc[-1]
        analysis=(f"【底背离看涨】近60日价格从{c.iloc[p1]:.2f}跌至{c.iloc[p2]:.2f}(跌幅{pct_p}%)创新低，"
                  f"但MACD DIF从{dif.iloc[d1]:.3f}升至{dif.iloc[d2]:.3f}(升幅{pct_d}%)未同步新低，"
                  f"形成标准底背离结构。RSI={r:.1f}处于{'低位' if r<40 else '中性'}区间，"
                  f"信号强度{s}分。建议关注后续放量确认，可分批建仓。")
        return True,s,analysis
    return False,0,''

def detect_top_divergence(df):
    """
    顶背离: 价格创新高，MACD DIF未创新高 → 看跌反转信号
    """
    if len(df) < 40: return False,0,''
    c=df['close'].tail(60); dif,_,_=macd(c); o=6
    ph=find_highs(c.values,o); dh=find_highs(dif.values,o)
    if len(ph)<2 or len(dh)<2: return False,0,''
    p1,p2=ph[-2],ph[-1]
    if c.iloc[p2]<=c.iloc[p1]: return False,0,''
    rdh=[i for i in dh if abs(i-p1)<o or abs(i-p2)<o]
    if len(rdh)<2: return False,0,''
    d1,d2=rdh[-2],rdh[-1]
    if dif.iloc[d2]<dif.iloc[d1]:
        s=round(min(100,abs(dif.iloc[d1]-dif.iloc[d2])/max(abs(dif.iloc[d1]),0.0001)*100+50),1)
        pct_p=round((c.iloc[p2]-c.iloc[p1])/c.iloc[p1]*100,2)
        r=rsi(c).iloc[-1]
        analysis=(f"【顶背离看跌】近60日价格从{c.iloc[p1]:.2f}涨至{c.iloc[p2]:.2f}(涨幅{pct_p}%)创新高，"
                  f"但MACD DIF从{dif.iloc[d1]:.3f}降至{dif.iloc[d2]:.3f}未同步新高，"
                  f"形成标准顶背离结构。RSI={r:.1f}处于{'高位' if r>60 else '中性'}区间，"
                  f"信号强度{s}分。建议逢高减仓，注意风险。")
        return True,s,analysis
    return False,0,''

def detect_uptrend(df):
    """
    上升趋势(左侧交易): 上升趋势中回调到支撑位 → 左侧买入机会
    适合在上升趋势中趁回调分批建仓
    """
    if len(df) < 40: return False,0,''
    c=df['close'].tail(80)
    ma5,ma10,ma20,ma60=ma(c,5),ma(c,10),ma(c,20),ma(c,60)
    dif,dea,_=macd(c); r=rsi(c)
    score=0; notes=[]
    m5=ma5.iloc[-1] if not pd.isna(ma5.iloc[-1]) else 0
    m10=ma10.iloc[-1] if not pd.isna(ma10.iloc[-1]) else 0
    m20=ma20.iloc[-1] if not pd.isna(ma20.iloc[-1]) else 0
    m60=ma60.iloc[-1] if not pd.isna(ma60.iloc[-1]) else 0
    cur=c.iloc[-1]

    # 趋势确认：中长期均线多头
    if m20>m60: score+=15; notes.append("MA20>MA60中期趋势向上")
    if m10>m20>m60: score+=15; notes.append("均线多头排列")
    # MACD确认趋势方向
    if dif.iloc[-1]>0 and dea.iloc[-1]>0: score+=10; notes.append("MACD零轴上方")
    # 左侧交易核心：当前价格回调到支撑位
    if cur<m10 and cur>m20: score+=20; notes.append("价格回调至MA10-MA20区间(左侧买点)")
    elif cur<m20 and cur>m60: score+=15; notes.append("价格回调至MA20-MA60区间(深度左侧买点)")
    elif cur<m5 and cur>m10: score+=10; notes.append("价格小幅回调至MA5-MA10区间")
    # 回调幅度确认
    high20=c.iloc[-20:].max()
    if high20>0:
        pullback=(high20-cur)/high20*100
        if 3<pullback<12: score+=15; notes.append(f"从20日高点回调{pullback:.1f}%(健康回调)")
        elif 12<=pullback<20: score+=10; notes.append(f"从20日高点回调{pullback:.1f}%(较深回调)")
    # RSI不能过热
    rsiv=r.iloc[-1]
    if 35<rsiv<65: score+=10; notes.append(f"RSI={rsiv:.1f}中性区间")
    elif rsiv<35: score+=8; notes.append(f"RSI={rsiv:.1f}偏弱(超跌反弹机会)")
    # 趋势延续性
    h=len(c)//2
    if c.iloc[h:].max()>c.iloc[:h].max() and c.iloc[h:].min()>c.iloc[:h].min():
        score+=10; notes.append("高低点同步抬高")

    if score>=50:
        analysis=f"【上升趋势·左侧交易】{'；'.join(notes[:5])}。当前价{cur:.2f}，MA20={m20:.2f}，MA60={m60:.2f}。综合评分{score}分。适合在上升趋势回调时分批左侧建仓，止损设于MA60({m60:.2f})下方。"
        return True,score,analysis
    return False,0,''

def detect_first_limit_up(df, market='A'):
    """
    首板: 今日首次涨停，前5日无涨停记录，量能放大
    """
    if len(df) < 8: return False,0,''
    c=df['close'].values; v=df['volume'].values
    th=9.5 if market=='A' else 15.0
    chg=(c[-1]/c[-2]-1)*100
    if chg<th: return False,0,''
    prev=[(c[i+1]/c[i]-1)*100 for i in range(-6,-1)]
    if any(x>=th for x in prev): return False,0,''
    avg=np.mean(v[-10:-1]); vr=v[-1]/avg if avg>0 else 0
    if vr<1.5: return False,0,''
    s=min(100,vr*30+chg*3)
    analysis=(f"【首板突破】今日涨幅{chg:.1f}%首次涨停，成交量放大至前5日均量的{vr:.1f}倍，"
              f"前5日无涨停记录，属于首次涨停突破形态。信号强度{round(s,1)}分。"
              f"关注次日能否连板，若高开高走可短线参与。")
    return True,round(s,1),analysis

def detect_consecutive_limit_up(df, market='A'):
    """
    连板: 连续涨停≥2天
    """
    if len(df) < 3: return False,0,''
    c=df['close'].values; th=9.5 if market=='A' else 15.0
    n=0; changes=[]
    for i in range(len(c)-1,0,-1):
        chg=(c[i]/c[i-1]-1)*100
        if chg>=th: n+=1; changes.append(round(chg,1))
        else: break
    if n<2: return False,n,''
    total=round((c[-1]/c[-n-1]-1)*100,2) if n<len(c)-1 else 0
    analysis=(f"【{n}连板】连续{n}日涨停，每日涨幅: {'→'.join(str(x)+'%' for x in reversed(changes))}，"
              f"累计涨幅{total}%。{'注意高位追涨风险，' if n>=5 else ''}"
              f"关注成交量变化，若放量开板需及时止盈。")
    return True,n,analysis

def detect_bottom_launch(df):
    """
    底部即将启动: 缩量筑底+MACD/KDJ金叉+布林收窄
    """
    if len(df) < 40: return False,0,''
    r=df.tail(80); c,h,l,v=r['close'],r['high'],r['low'],r['volume']
    score=0; notes=[]
    hf=len(c)//2
    if c.iloc[hf:].mean()<c.iloc[:hf].mean()*0.95: score+=15; notes.append("前期有下跌趋势")
    vr=v.iloc[-10:].mean(); vb=v.iloc[-30:-10].mean()
    if vb>0 and vr<vb*0.8: score+=20; notes.append(f"近10日均量缩至前期的{round(vr/vb*100,0)}%(筑底特征)")
    rg=(c.iloc[-20:].max()-c.iloc[-20:].min())/c.iloc[-20:].mean()*100
    if rg<15: score+=15; notes.append(f"20日振幅仅{rg:.1f}%(窄幅盘整)")
    dif,dea,_=macd(c)
    if dif.iloc[-2]<=dea.iloc[-2] and dif.iloc[-1]>dea.iloc[-1]:
        score+=20; notes.append("MACD刚刚金叉")
    elif dif.iloc[-1]<dea.iloc[-1] and dif.iloc[-1]>dif.iloc[-2] and abs(dea.iloc[-1]-dif.iloc[-1])<abs(dea.iloc[-1])*0.05:
        score+=15; notes.append("MACD即将金叉")
    kk,dd,_=kdj(h,l,c)
    if kk.iloc[-1]<30 and dd.iloc[-1]<30 and kk.iloc[-1]>dd.iloc[-1] and kk.iloc[-2]<=dd.iloc[-2]:
        score+=15; notes.append(f"KDJ低位金叉(K={kk.iloc[-1]:.1f})")
    elif kk.iloc[-1]<30: score+=8; notes.append(f"KDJ超卖(K={kk.iloc[-1]:.1f})")
    _,_,_,bw=boll(c)
    if bw.iloc[-1]<bw.iloc[-20:].mean()*0.7:
        score+=15; notes.append("布林带明显收窄(变盘前兆)")

    if score>=55:
        cur=c.iloc[-1]
        analysis=f"【底部即将启动】{'；'.join(notes)}。当前价{cur:.2f}，综合评分{score}分。多个底部信号共振，建议密切关注放量突破确认，可在突破时右侧跟进或左侧轻仓试探。"
        return True,score,analysis
    return False,0,''

def detect_potential_first_board(df, market='A'):
    """
    次日可能首板: 今日未涨停但强势上攻，量价配合预示次日有涨停潜力
    条件: 涨幅3-9%，放量1.5x以上，MACD金叉或强势，RSI适中，均线多头初期
    """
    if len(df) < 15: return False,0,''
    c=df['close'].values; v=df['volume'].values
    th=9.5 if market=='A' else 15.0
    today_chg=(c[-1]/c[-2]-1)*100
    # 今日未涨停但涨幅可观
    if today_chg>=th: return False,0,''  # 已涨停的不算"可能首板"
    if today_chg<3: return False,0,''   # 涨幅太小
    # 量能放大
    avg5=np.mean(v[-6:-1]); vr=v[-1]/avg5 if avg5>0 else 0
    if vr<1.5: return False,0,''
    score=0; notes=[]
    if 3<=today_chg<5: score+=15; notes.append(f"今日涨幅{today_chg:.1f}%")
    elif 5<=today_chg<7: score+=20; notes.append(f"今日涨幅{today_chg:.1f}%(强势)")
    elif today_chg>=7: score+=25; notes.append(f"今日涨幅{today_chg:.1f}%(逼近涨停)")
    notes.append(f"量比{vr:.1f}倍")
    # MACD
    df2=df.tail(60); cc=df2['close']
    dif,dea,_=macd(cc)
    if dif.iloc[-1]>dea.iloc[-1] and dif.iloc[-2]<=dea.iloc[-2]: score+=20; notes.append("MACD今日金叉")
    elif dif.iloc[-1]>dea.iloc[-1] and dif.iloc[-1]>dif.iloc[-2]: score+=15; notes.append("MACD多头向上")
    elif dif.iloc[-1]>0: score+=10; notes.append("MACD零轴上")
    # 均线
    ma5v=ma(cc,5).iloc[-1]; ma20v=ma(cc,20).iloc[-1]
    if c[-1]>ma5v>ma20v: score+=15; notes.append("站上MA5和MA20")
    elif c[-1]>ma5v: score+=10
    # RSI
    r=rsi(cc).iloc[-1]
    if 55<r<75: score+=15; notes.append(f"RSI={r:.1f}强势区间")
    elif 45<r<=55: score+=10; notes.append(f"RSI={r:.1f}中性偏强")
    # 近5日价量配合
    up_days=sum(1 for i in range(-5,0) if (c[i]/c[i-1]-1)*100>0)
    score+=up_days*3; notes.append(f"近5日{up_days}日收阳")
    # 接近阶段高点
    high20=cc.iloc[-20:].max()
    if cc.iloc[-1]>=high20*0.95: score+=10; notes.append("接近20日高点(突破在即)")

    if score>=55:
        analysis=(f"【次日可能首板】{'；'.join(notes)}。综合评分{score}分。"
                  f"今日量价配合良好，若明日高开且量能持续放大，涨停概率较大，短线可关注竞价强度。")
        return True,score,analysis
    return False,0,''

def detect_potential_continue_board(df, market='A'):
    """
    次日可能再板: 已连续涨停1-3天，封板质量好预示次日有望继续连板
    条件: 连板1-3天，封板量能健康，换手适中，非高位放量
    """
    if len(df) < 8: return False,0,''
    c=df['close'].values; v=df['volume'].values
    th=9.5 if market=='A' else 15.0
    # 先在连板中
    n=0
    for i in range(len(c)-1,0,-1):
        if (c[i]/c[i-1]-1)*100>=th: n+=1
        else: break
    if n<1 or n>4: return False,0,''  # 1-4连板才有"再板"分析价值
    score=0; notes=[]
    notes.append(f"已{n}连板")
    score+=min(n*15,45)
    # 最新涨停日量能分析
    latest_vol=v[-1]; prev_vol=v[-2] if len(v)>2 else 0
    if prev_vol>0:
        vol_ratio=latest_vol/prev_vol
        if vol_ratio<0.7: score+=20; notes.append(f"缩量涨停(量比{vol_ratio:.2f},封板牢固)")
        elif vol_ratio<1.5: score+=10; notes.append(f"量能平稳(量比{vol_ratio:.2f})")
        else: score+=5; notes.append(f"放量涨停(量比{vol_ratio:.2f},关注是否出货)")
    # 累计涨幅
    total_chg=(c[-1]/c[-n-1]-1)*100 if n<len(c)-1 else 0
    notes.append(f"累计涨幅{total_chg:.1f}%")
    # 对比前几天均量
    pre_vol=v[-n-5:-n] if len(v)>n+5 else v[:-n]
    avg_pre=np.mean(pre_vol) if len(pre_vol)>0 else 0
    avg_board=np.mean(v[-n:])
    if avg_pre>0 and avg_board>avg_pre*1.5: score+=10; notes.append("连板期间整体放量(资金关注)")
    # 连板天数加分
    if n==1: score+=15; notes.append("首板次日惯性最强")
    elif n==2: score+=10; notes.append("2连板仍有空间")
    elif n==3: score+=5
    # RSI
    df2=df.tail(60)
    r=rsi(df2['close']).iloc[-1]
    if r<80: score+=10; notes.append(f"RSI={r:.1f}未严重超买")
    else: notes.append(f"RSI={r:.1f}高位(注意风险)")

    if score>=45:
        analysis=(f"【次日可能再板】{'；'.join(notes)}。综合评分{score}分。"
                  f"{'封板质量好，次日高开概率大，' if n<=2 else '连板数偏多，'}短线关注集合竞价情况，若低开放量需警惕断板风险。")
        return True,score,analysis
    return False,0,''

def analyze_sentiment(df, stock_info=None):
    """
    情绪因子分析：从量价数据中提取市场情绪指标
    返回: (情绪总分0-100, 情绪描述文字, 详细指标字典)
    """
    if len(df) < 10: return 50, '数据不足', {}
    c = df['close'].values; v = df['volume'].values
    h = df['high'].values; l = df['low'].values; o = df['open'].values

    score = 50; notes = []; indicators = {}

    # 1. 连阳/连阴情绪 (占20分)
    up_days = 0; down_days = 0
    for i in range(-1, -8, -1):
        if abs(i) > len(c): break
        chg = (c[i] / c[i-1] - 1) * 100
        if chg > 0: up_days += 1; down_days = 0
        else: down_days += 1; up_days = 0
    indicators['consec_up'] = up_days if up_days >= 2 else -down_days if down_days >= 2 else 0
    if up_days >= 3: score += 15; notes.append(f"连阳{up_days}日(多头情绪强)")
    elif up_days >= 1: score += 5; notes.append(f"近{up_days}日偏多")
    elif down_days >= 3: score -= 15; notes.append(f"连阴{down_days}日(空头情绪重)")
    elif down_days >= 1: score -= 5

    # 2. 近期涨跌幅情绪 (占20分)
    chg5 = (c[-1] / c[-6] - 1) * 100 if len(c) > 5 else 0
    chg10 = (c[-1] / c[-11] - 1) * 100 if len(c) > 10 else 0
    indicators['chg5d'] = round(chg5, 2); indicators['chg10d'] = round(chg10, 2)
    if chg5 > 10: score += 15; notes.append(f"5日涨{chg5:.1f}%(情绪亢奋)")
    elif chg5 > 5: score += 10; notes.append(f"5日涨{chg5:.1f}%(情绪积极)")
    elif chg5 > 0: score += 5; notes.append("5日温和上涨")
    elif chg5 < -10: score -= 15; notes.append(f"5日跌{abs(chg5):.1f}%(情绪恐慌)")
    elif chg5 < -5: score -= 10; notes.append(f"5日跌{abs(chg5):.1f}%(情绪悲观)")
    elif chg5 < 0: score -= 3

    # 3. 量能情绪 (占20分)——量能放大=关注度上升
    v5 = np.mean(v[-6:-1]); v20 = np.mean(v[-21:-1]) if len(v)>21 else v5
    vol_ratio_5 = v[-1] / v5 if v5 > 0 else 1
    vol_ratio_20 = v[-1] / v20 if v20 > 0 else 1
    indicators['vol_ratio_5d'] = round(vol_ratio_5, 2)
    indicators['vol_ratio_20d'] = round(vol_ratio_20, 2)
    if vol_ratio_5 > 2.5: score += 18; notes.append(f"量能激增至5日均量{vol_ratio_5:.1f}倍(市场高度关注)")
    elif vol_ratio_5 > 1.8: score += 12; notes.append(f"放量至5日均量{vol_ratio_5:.1f}倍(关注度上升)")
    elif vol_ratio_5 > 1.2: score += 6; notes.append("温和放量")
    elif vol_ratio_5 < 0.5: score -= 8; notes.append("严重缩量(关注度低)")
    elif vol_ratio_5 < 0.7: score -= 3; notes.append("缩量(交投清淡)")

    # 4. 日内振幅情绪 (占15分)——高振幅=分歧大/情绪激烈
    amp = (h[-1] / l[-1] - 1) * 100 if l[-1] > 0 else 0
    avg_amp = np.mean([(h[i]/l[i]-1)*100 for i in range(-10, -1) if l[i] > 0]) if len(h) > 10 else amp
    indicators['amplitude'] = round(amp, 2)
    if amp > avg_amp * 2 and c[-1] > o[-1]:
        score += 12; notes.append(f"日内振幅{amp:.1f}%(大振幅收阳,多空激烈多方胜)")
    elif amp > avg_amp * 2:
        score -= 8; notes.append(f"日内振幅{amp:.1f}%(大振幅收阴,分歧加剧)")
    elif amp < 2: score += 5; notes.append(f"振幅仅{amp:.1f}%(筹码稳定)")

    # 5. 换手率情绪 (占15分)——优先stock_info，回退历史数据
    turnover = stock_info.get('turnover', 0) if stock_info else 0
    if turnover == 0 and 'turnover' in df.columns:
        turnover = float(df['turnover'].iloc[-1]) if len(df['turnover']) > 0 else 0
    indicators['turnover'] = round(turnover, 2)
    if turnover > 15: score += 12; notes.append(f"换手率{turnover:.1f}%(超高换手,极度活跃)")
    elif turnover > 8: score += 10; notes.append(f"换手率{turnover:.1f}%(高换手,活跃)")
    elif turnover > 3: score += 6; notes.append(f"换手率{turnover:.1f}%(正常活跃)")
    elif turnover > 1: score += 2
    elif turnover > 0 and turnover < 0.5: score -= 5; notes.append(f"换手率{turnover:.1f}%(极度冷清)")

    # 6. 涨速动量 (占10分)——盘中涨速反映即时情绪
    if stock_info:
        chg_speed = stock_info.get('change_pct', 0)
        if chg_speed > 7: score += 10; notes.append("涨速极快(抢筹情绪)")
        elif chg_speed > 4: score += 7; notes.append("涨速较快(做多积极)")
        elif chg_speed > 1: score += 3
        elif chg_speed < -7: score -= 10; notes.append("跌速极快(恐慌抛售)")
        elif chg_speed < -4: score -= 7; notes.append("跌速较快(情绪偏空)")

    # 归一化到0-100
    score = max(0, min(100, score))
    # 生成情绪总结
    if score >= 80: level = '极度亢奋'; icon = '🔥🔥'
    elif score >= 65: level = '积极乐观'; icon = '🔥'
    elif score >= 50: level = '温和偏多'; icon = '😊'
    elif score >= 35: level = '中性观望'; icon = '😐'
    elif score >= 20: level = '偏空谨慎'; icon = '😟'
    else: level = '极度恐慌'; icon = '❄️'

    summary = f"{icon}情绪评分{score}分({level})。{'；'.join(notes[-4:])}。" if notes else f"{icon}情绪评分{score}分({level})。"
    return score, summary, indicators

# ========== 分析入口 ==========

def analyze_stock(stock, market='A', patterns=None, auction_data=None):
    if patterns is None:
        patterns = ['bottom_divergence','uptrend','first_limit_up',
                    'consecutive_limit_up','top_divergence','bottom_launch',
                    'potential_first_board','potential_continue_board']
    h = fetch_stock_history(stock['code'], market, 60)

    # 构造结果基础字段
    latest = 0; prev = 0; chg = 0
    if h is not None and len(h['close']) >= 30:
        latest = h['close'][-1]; prev = h['close'][-2] if len(h['close'])>=2 else latest
        chg = round((latest/prev-1)*100,2)
    elif auction_data:
        latest = auction_data.get('latest', auction_data.get('auction_price', 0)) or 0
        prev = auction_data.get('prev_close', 0)
        chg = auction_data.get('change_pct', 0) or round((latest/prev-1)*100,2) if latest and prev else 0

    r = {'code':stock['code'],'name':stock.get('name',''),'price':round(latest,2) if latest else 0,
         'change_pct':chg,'market':market,'patterns':[],
         'sentiment':{'score':50,'summary':'','indicators':{}}}

    # K线数据有效时才做K线类形态分析
    if h is not None and len(h['close']) >= 30:
        df = pd.DataFrame({'open':h['open'],'close':h['close'],'high':h['high'],
                           'low':h['low'],'volume':h['volume'],
                           'turnover':h.get('turnover', [0]*len(h['close']))})

        sent_score, sent_summary, sent_indicators = analyze_sentiment(df, stock)
        r['sentiment'] = {'score':sent_score,'summary':sent_summary,'indicators':sent_indicators}

        checks = {
            'bottom_divergence': ('底背离','buy',detect_bottom_divergence),
            'uptrend': ('上升趋势(左侧)','buy',detect_uptrend),
            'first_limit_up': ('首板','buy',lambda d: detect_first_limit_up(d, market)),
            'consecutive_limit_up': ('连板','buy',lambda d: detect_consecutive_limit_up(d, market)),
            'top_divergence': ('顶背离','sell',detect_top_divergence),
            'bottom_launch': ('底部即将启动','buy',detect_bottom_launch),
            'potential_first_board': ('次日可能首板','buy',lambda d: detect_potential_first_board(d, market)),
            'potential_continue_board': ('次日可能再板','buy',lambda d: detect_potential_continue_board(d, market)),
        }
        for p in patterns:
            if p in checks:
                name,sig,fn = checks[p]
                ok,val,analysis = fn(df)
                if ok:
                    dis = name
                    if p == 'consecutive_limit_up': dis = f'连板({val}连板)'; val = val*20
                    full_analysis = analysis + ' ' + sent_summary
                    r['patterns'].append({
                        'type':p,'name':dis,'strength':round(val,1),
                        'signal':sig,'analysis':full_analysis
                    })

    return r if r['patterns'] else None

# ========== API路由 ==========

@app.route('/')
def index():
    return HTML_PAGE

@app.route('/api/health')
def api_health():
    """全面健康检查"""
    ok = True; details = {}
    # baostock连接
    try:
        ensure_baostock()
        details['baostock'] = 'OK'
    except Exception as e:
        details['baostock'] = f'FAIL: {e}'; ok = False
    # 缓存状态
    details['cache_entries'] = len(_cache)
    details['cache_size'] = f'{sum(len(str(v)) for v in _cache.values())//1024}KB'
    # 内存
    try:
        import psutil
        mem = psutil.Process().memory_info().rss // 1024 // 1024
        details['memory_mb'] = mem
        if mem > 400: details['memory_warning'] = '接近512MB上限，建议重启'
    except: details['memory_mb'] = '未知'
    details['baostock_fails'] = _bs_fail_count
    # 快速测试各API
    try:
        h = fetch_stock_history('000001', 'A', 30)
        details['kline_test'] = f'OK ({len(h["close"])}条)' if h and len(h.get('close',[]))>0 else 'EMPTY'
    except Exception as e: details['kline_test'] = f'FAIL: {e}'
    try:
        detail = fetch_fundamental_data('000001')
        details['fund_test'] = f'OK (行业:{detail.get("industry","?")})' if detail else 'EMPTY'
    except Exception as e: details['fund_test'] = f'FAIL: {e}'
    return jsonify({'ok': ok, 'details': details})

@app.route('/api/test')
def api_test():
    results = {}; t0 = time.time()

    # 数据源状态
    results['数据源状态'] = {
        'tushare': '✅ 已启用' if TUSHARE_AVAILABLE else '❌ 未启用',
        'baostock': '✅ 备用中',
        '东方财富': '✅ 港股/实时行情',
        '当前A股数据源': 'Tushare Pro' if TUSHARE_AVAILABLE else 'Baostock'
    }

    try:
        bs.login()
        results['baostock登录'] = f'OK (备用)'
    except Exception as e:
        results['baostock登录'] = f'FAIL: {e}'
    try:
        sl = fetch_a_stock_list()
        results['A股列表'] = f'OK ({len(sl)}只)'
    except Exception as e:
        results['A股列表'] = f'FAIL: {e}'
    try:
        h = fetch_stock_history('000001','A',90)
        results['A股K线(平安银行)'] = f'OK ({len(h["close"])}条)' if h else 'FAIL'
    except Exception as e:
        results['A股K线'] = f'FAIL: {e}'
    try:
        sl = fetch_hk_connect_list()
        results['港股通标的'] = f'OK ({len(sl)}只)'
    except Exception as e:
        results['港股通标的'] = f'FAIL: {e}'

    # Tushare积分信息
    if TUSHARE_AVAILABLE:
        try:
            # 查询积分
            info = tushare_pro.query('user_info')
            if info is not None and len(info) > 0:
                results['Tushare积分'] = f'{info.iloc[0].get("积分", "未知")}'
        except:
            results['Tushare积分'] = '查询失败'

    return jsonify({'results':results,'logs':LOG[-20:]})

@app.route('/api/scan', methods=['POST'])
def api_scan():
    global SCAN_STATUS
    # 缓存超过10000条时清理旧的（防止内存爆炸）
    if len(_cache) > 10000:
        with _cache_lock:
            keys = sorted(_cache.keys(), key=lambda k: _cache[k][1])  # 按时间排序
            for old_key in keys[:2000]:  # 删最旧的2000条
                if old_key in _cache: del _cache[old_key]
        log(f"缓存清理: {len(_cache)}条")

    data = request.get_json() or {}
    markets = data.get('markets', ['A'])
    patterns = data.get('patterns', None)
    batch_size = data.get('batch_size', 100)
    offset = data.get('offset', 0)

    t0 = time.time()
    results = []; total_available = 0; scanned = 0

    for market in markets:
        try:
            sl = fetch_a_stock_list() if market == 'A' else fetch_hk_connect_list()
        except: continue
        if not sl: continue
        sl = sorted(sl, key=lambda x: x.get('amount', 0), reverse=True)
        total_available += len(sl)
        batch = sl[offset:offset+batch_size]
        scanned += len(batch)
        log(f"扫描: {market} offset={offset} batch={batch_size}")

        # 排队分析(单线程+锁保护，避免baostock并发死锁)
        for s in batch:
            try:
                a = analyze_stock(s, market, patterns)
                if a: results.append(a)
            except: pass

    results.sort(key=lambda x: (len(x['patterns']),
                 max((p.get('strength',0) for p in x['patterns']),default=0)), reverse=True)
    elapsed = time.time() - t0
    has_more = (offset + batch_size) < total_available

    SCAN_STATUS = {'running':False, 'progress': offset+batch_size, 'total': total_available, 'matched': len(results)}
    log(f"分页扫描: offset={offset}, 匹配{len(results)}只, 耗时{elapsed:.1f}s, 还有更多={has_more}")

    return jsonify({
        'success':True, 'total_scanned': scanned, 'total_matches': len(results),
        'total_available': total_available, 'offset': offset, 'has_more': has_more,
        'results':results, 'logs':LOG[-15:], 'elapsed':round(elapsed,1)
    })

@app.route('/api/position', methods=['POST'])
def api_position():
    """持仓量化分析"""
    data = request.get_json() or {}
    code = data.get('code', '').strip()
    market = data.get('market', 'A')
    buy_price = data.get('buy_price', 0)
    shares = data.get('shares', 0)
    target_amount = data.get('target_amount', None)  # 可选: 目标盈利金额

    if not code or not buy_price or not shares:
        return jsonify({'success': False, 'message': '请填写完整的持仓信息(代码/买入价/股数)'}), 200

    try:
        log(f"持仓分析: {code} 买入{buy_price}×{shares}股")
        h = fetch_stock_history(code, market, 120)
        if h is None: return jsonify({'success': False, 'message': f'无法获取{code}的历史数据'}), 200

        cur = h['close'][-1]
        df = pd.DataFrame({'close': h['close'], 'high': h['high'], 'low': h['low'],
                           'volume': h['volume']})
        close = df['close']

        # 计算指标供分析用
        dif, dea, _ = macd(close)
        r = rsi(close)
        indicators = {
            'rsi': round(float(r.iloc[-1]), 1),
            'ma20': round(float(ma(close, 20).iloc[-1]), 2),
            'ma60': round(float(ma(close, 60).iloc[-1]), 2),
            'macd_bar': round(float(dif.iloc[-1] - dea.iloc[-1]), 4),
        }

        result = analyze_position(code, market, buy_price, shares, cur, indicators, target_amount)

        # 如果有目标盈利，生成路线图
        if target_amount and target_amount > 0:
            close_s = pd.Series(h['close']); high_s = pd.Series(h['high']); low_s = pd.Series(h['low'])
            # 计算ATR
            tr = pd.DataFrame({'hl':high_s-low_s,'hpc':abs(high_s-close_s.shift(1)),'lpc':abs(low_s-close_s.shift(1))}).max(axis=1)
            atr_val = float(tr.tail(14).mean())
            atr_pct_val = round(atr_val / cur * 100, 2)
            low60 = float(low_s.tail(60).min()); high60 = float(high_s.tail(60).max())
            ma60_v = float(ma(close_s, 60).iloc[-1]); ma20_v = float(ma(close_s, 20).iloc[-1])
            supports = [{'level': round(ma20_v, 2)}, {'level': round(ma60_v, 2)}, {'level': round(low60, 2)}]
            resistances = [{'level': round(high60, 2)}, {'level': round(cur * 1.05, 2)}, {'level': round(cur * 1.1, 2)}]
            route = plan_route_to_target(buy_price, shares, cur, target_amount, supports, resistances, atr_pct_val)
            result['route_plan'] = route

        result['name'] = result.get('company_name', code)

        return jsonify({'success': True, 'result': result})
    except Exception as e:
        log(f"持仓分析异常: {traceback.format_exc()}")
        return jsonify({'success': False, 'message': f'分析出错: {str(e)}'}), 200

@app.route('/api/scan_status')
def scan_status():
    return jsonify(SCAN_STATUS)

# ========== 单股深度分析 + 投资建议 ==========

def generate_recommendation(patterns, sentiment, indicators):
    """综合所有因子生成投资建议"""
    reasons = []; risks = []; score = 50

    # 统计多空信号
    buy_signals = [p for p in patterns if p['signal'] == 'buy']
    sell_signals = [p for p in patterns if p['signal'] == 'sell']

    # 形态因子 (占40分)
    if len(buy_signals) >= 3: score += 30; reasons.append(f"同时触发{len(buy_signals)}个看涨形态，多头共振强烈")
    elif len(buy_signals) >= 2: score += 20; reasons.append(f"触发{len(buy_signals)}个看涨形态，多头信号明确")
    elif len(buy_signals) >= 1: score += 10; reasons.append(f"触发看涨形态「{buy_signals[0]['name']}」")
    if sell_signals:
        score -= len(sell_signals) * 12; risks.append(f"触发{len(sell_signals)}个看跌形态，需警惕回调风险")

    # 情绪因子 (占25分)
    sent_score = sentiment.get('score', 50)
    if sent_score >= 80: score += 20; reasons.append("市场情绪极度亢奋，短线动能充足")
    elif sent_score >= 65: score += 15; reasons.append("市场情绪积极乐观，资金关注度高")
    elif sent_score >= 50: score += 8; reasons.append("市场情绪温和偏多")
    elif sent_score < 35: score -= 15; risks.append("市场情绪偏冷，交投清淡，流动性风险需关注")
    elif sent_score < 50: score -= 5; risks.append("市场情绪偏空")

    # 技术指标因子 (占35分)
    rsi_val = indicators.get('rsi', 50)
    if 40 <= rsi_val <= 60: score += 10; reasons.append(f"RSI={rsi_val:.0f}处于中性区间，无超买超卖压力")
    elif 30 <= rsi_val < 40: score += 8; reasons.append(f"RSI={rsi_val:.0f}偏低，存在超跌反弹机会")
    elif rsi_val > 75: score -= 8; risks.append(f"RSI={rsi_val:.0f}严重超买，短线回调压力大")
    elif rsi_val > 65: score -= 3; risks.append(f"RSI={rsi_val:.0f}偏高，追高需谨慎")
    elif rsi_val < 25: score += 5; reasons.append(f"RSI={rsi_val:.0f}极度超卖，反弹概率较大")

    # MACD判断
    macd_dif = indicators.get('macd_dif', 0)
    macd_dea = indicators.get('macd_dea', 0)
    if macd_dif > macd_dea and macd_dif > 0: score += 8; reasons.append("MACD零轴上金叉运行，趋势偏多")
    elif macd_dif > macd_dea: score += 5; reasons.append("MACD金叉中，短期偏多")
    elif macd_dif < macd_dea and macd_dif > 0: score -= 3; risks.append("MACD零轴上死叉，短期调整中")
    elif macd_dif < 0: score -= 8; risks.append("MACD零轴下运行，趋势偏弱")

    # 均线判断
    ma5 = indicators.get('ma5', 0)
    ma20 = indicators.get('ma20', 0)
    ma60 = indicators.get('ma60', 0)
    price = indicators.get('price', 0)
    if price > ma20 > ma60: score += 7; reasons.append(f"价格{price:.2f}站上MA20({ma20:.2f})和MA60({ma60:.2f})，中期趋势向上")
    elif price > ma20: score += 3
    elif price < ma60: score -= 5; risks.append(f"价格低于MA60({ma60:.2f})，中长期趋势偏弱")

    # KD指标
    k_val = indicators.get('kdj_k', 50)
    if k_val < 20: score += 5; reasons.append(f"KDJ-K={k_val:.0f}超卖区，短线反弹可期")
    elif k_val > 85: score -= 5; risks.append(f"KDJ-K={k_val:.0f}超买区，短线注意回落")

    # 布林带
    bw = indicators.get('boll_bandwidth', 5)
    if bw < 3: reasons.append(f"布林带宽{bw:.1f}%极窄，变盘在即")

    # 归一化
    score = max(0, min(100, round(score)))
    # 综合等级
    if score >= 80: level = '强烈看多'; action = '可积极建仓/加仓，设置移动止盈'; icon = '🚀'
    elif score >= 65: level = '偏多看多'; action = '可分批建仓，控制仓位5-7成'; icon = '📈'
    elif score >= 50: level = '中性偏多'; action = '可轻仓试探，等待更强信号确认'; icon = '📊'
    elif score >= 35: level = '中性偏空'; action = '建议观望为主，已有持仓注意风险'; icon = '⚠️'
    elif score >= 20: level = '偏空看空'; action = '不建议新建仓位，已有持仓考虑减仓'; icon = '🔻'
    else: level = '强烈看空'; action = '建议回避，持币观望'; icon = '🛑'

    summary = f"{icon} 综合评分{score}分，评级: {level}。{action}。"
    return {'score': score, 'level': level, 'action': action, 'icon': icon,
            'summary': summary, 'reasons': reasons, 'risks': risks}

def predict_targets(df, indicators):
    """基于技术指标预测买入点、卖出点和上涨空间"""
    close = df['close']; high = df['high']; low = df['low']
    cur = float(close.iloc[-1])

    # ATR (14日平均真实波幅)
    tr = pd.DataFrame({
        'h_l': high - low,
        'h_pc': abs(high - close.shift(1)),
        'l_pc': abs(low - close.shift(1))
    }).max(axis=1)
    atr = float(tr.tail(14).mean())

    # === 支撑位(买入点) ===
    supports = []
    # S1: MA20
    ma20_val = indicators.get('ma20', 0)
    if ma20_val > 0 and ma20_val < cur: supports.append({'level': round(ma20_val, 2), 'label': 'MA20均线支撑', 'strength': '中等'})
    # S2: MA60
    ma60_val = indicators.get('ma60', 0)
    if ma60_val > 0 and ma60_val < cur: supports.append({'level': round(ma60_val, 2), 'label': 'MA60均线强支撑', 'strength': '强'})
    # S3: 布林下轨
    bl = indicators.get('boll_lower', 0)
    if bl > 0 and bl < cur: supports.append({'level': round(bl, 2), 'label': '布林带下轨', 'strength': '强'})
    # S4: 60日最低点
    low60 = float(low.tail(60).min())
    if low60 < cur: supports.append({'level': round(low60, 2), 'label': '60日最低点(极限支撑)', 'strength': '极强'})
    # S5: 近期回调低点
    low20 = float(low.tail(20).min())
    if low20 < cur and low20 not in [s['level'] for s in supports]:
        supports.append({'level': round(low20, 2), 'label': '20日低点', 'strength': '较强'})

    # 去重排序(从高到低)
    supports = sorted({s['level']: s for s in supports}.values(), key=lambda x: x['level'], reverse=True)

    # 最佳买入区间
    buy_low = supports[1]['level'] if len(supports) > 1 else supports[0]['level'] if supports else cur * 0.95
    buy_high = supports[0]['level'] if supports else cur
    buy_zone = f"{buy_low:.2f} ~ {buy_high:.2f}"

    # === 阻力位(卖出点) ===
    resistances = []
    # R1: 布林上轨
    bu = indicators.get('boll_upper', 0)
    if bu > 0 and bu > cur: resistances.append({'level': round(bu, 2), 'label': '布林带上轨', 'strength': '中等'})
    # R2: 近期高点
    high20 = float(high.tail(20).max())
    if high20 > cur: resistances.append({'level': round(high20, 2), 'label': '20日高点', 'strength': '较强'})
    # R3: 60日高点
    high60 = float(high.tail(60).max())
    if high60 > cur and high60 != high20:
        resistances.append({'level': round(high60, 2), 'label': '60日高点(强阻力)', 'strength': '强'})
    # R4: ATR投影(1.5倍ATR)
    atr_target = cur + atr * 2.5
    resistances.append({'level': round(atr_target, 2), 'label': f'ATR短期目标(2.5×ATR)', 'strength': '参考'})

    # 去重排序(从低到高)
    resistances = sorted({r['level']: r for r in resistances}.values(), key=lambda x: x['level'])

    # 最佳卖出区间
    sell_low = resistances[0]['level'] if resistances else cur * 1.05
    sell_high = resistances[1]['level'] if len(resistances) > 1 else resistances[0]['level'] if resistances else cur * 1.1
    sell_zone = f"{sell_low:.2f} ~ {sell_high:.2f}"

    # === 上涨空间预测 ===
    # 目标1: 最接近的阻力位
    target1 = resistances[0]['level'] if resistances else cur * 1.05
    upside1 = round((target1 / cur - 1) * 100, 1)

    # 目标2: 远端的阻力位
    target2 = resistances[-1]['level'] if len(resistances) > 1 else cur * 1.1
    upside2 = round((target2 / cur - 1) * 100, 1)

    # 基于形态的调整
    pat_detected = False

    # 止损位(买入后如果跌破这个应该止损)
    stop_loss = round(buy_low * 0.97, 2) if buy_low > 0 else round(cur * 0.93, 2)
    stop_pct = round((stop_loss / cur - 1) * 100, 1)

    # 风险收益比
    risk = cur - stop_loss
    reward1 = target1 - cur
    reward2 = target2 - cur
    rr1 = round(reward1 / risk, 1) if risk > 0 else 0
    rr2 = round(reward2 / risk, 1) if risk > 0 else 0

    return {
        'current_price': round(cur, 2),
        'atr': round(atr, 2),
        'buy_zone': buy_zone,
        'buy_explanation': f"回调至{buy_zone}区间可分批建仓，该区间为均线与布林带共振支撑区",
        'supports': supports[:4],
        'sell_zone': sell_zone,
        'sell_explanation': f"上涨至{sell_zone}区间可分批止盈，该区间面临多重技术阻力",
        'resistances': resistances[:4],
        'stop_loss': stop_loss,
        'stop_loss_pct': stop_pct,
        'targets': [
            {'name': '短期目标', 'price': round(target1, 2), 'upside': upside1,
             'rr_ratio': rr1, 'timeframe': '1-2周',
             'method': '最近阻力位(布林上轨/前高)'},
            {'name': '中期目标', 'price': round(target2, 2), 'upside': upside2,
             'rr_ratio': rr2, 'timeframe': '2-4周',
             'method': '强阻力位(60日高点/ATR投影)'},
        ],
        'summary': (f"当前价{cur:.2f}，止损{stop_loss:.2f}({stop_pct}%)。"
                    f"最佳买入区间{buy_zone}，短期目标{target1:.2f}(+{upside1}%)，"
                    f"中期目标{target2:.2f}(+{upside2}%)。"
                    f"风险收益比: 短期{rr1}:1，中期{rr2}:1。")
    }

def predict_next_day(df, indicators):
    """预测次日走势区间和方向概率"""
    close = df['close']; high = df['high']; low = df['low']; vol = df['volume']
    cur = float(close.iloc[-1])

    # ATR计算
    tr = pd.DataFrame({'hl': high-low, 'hpc': abs(high-close.shift(1)), 'lpc': abs(low-close.shift(1))}).max(axis=1)
    atr = float(tr.tail(14).mean())
    atr_pct = round(atr / cur * 100, 2)

    # === 方向判断(多因子加权) ===
    direction_score = 0  # 正=偏多，负=偏空
    reasons = []

    # 1. MACD方向 (占25分)
    dif, dea, bar = macd(close)
    if bar.iloc[-1] > bar.iloc[-2]: direction_score += 12; reasons.append("MACD红柱放大")
    elif bar.iloc[-1] > 0: direction_score += 5
    elif bar.iloc[-1] < bar.iloc[-2]: direction_score -= 12; reasons.append("MACD绿柱放大")
    # DIF方向
    if dif.iloc[-1] > dif.iloc[-2]: direction_score += 8; reasons.append("DIF上行")
    elif dif.iloc[-1] < dif.iloc[-2]: direction_score -= 8; reasons.append("DIF下行")
    # MACD位置
    if dif.iloc[-1] > 0 and dea.iloc[-1] > 0: direction_score += 5
    elif dif.iloc[-1] < 0 and dea.iloc[-1] < 0: direction_score -= 5

    # 2. RSI方向 (占20分)
    r = rsi(close)
    rsi_now = float(r.iloc[-1]); rsi_prev = float(r.iloc[-2])
    if rsi_now > rsi_prev: direction_score += 10; reasons.append("RSI上升")
    else: direction_score -= 10; reasons.append("RSI下降")
    if 30 < rsi_now < 70: direction_score += 5  # 中性区间
    if rsi_now < 30: direction_score += 5; reasons.append("RSI超卖(反弹概率大)")
    if rsi_now > 75: direction_score -= 5; reasons.append("RSI超买(回调压力)")

    # 3. 量能趋势 (占20分)
    v5 = float(vol.tail(5).mean()); v20 = float(vol.tail(20).mean())
    if v5 > v20 * 1.2: direction_score += 12; reasons.append("近5日放量(资金介入)")
    elif v5 > v20: direction_score += 5
    elif v5 < v20 * 0.7: direction_score -= 8; reasons.append("近5日缩量(交投清淡)")
    # 今日量比
    today_vol = float(vol.iloc[-1]); prev_vol_avg = float(vol.iloc[-6:-1].mean())
    if prev_vol_avg > 0 and today_vol > prev_vol_avg * 1.5:
        if close.iloc[-1] > close.iloc[-2]: direction_score += 8; reasons.append("放量上涨(强势)")
        else: direction_score -= 5; reasons.append("放量下跌(弱势)")

    # 4. 近期动量 (占20分)
    chg1 = (close.iloc[-1]/close.iloc[-2]-1)*100
    chg5 = (close.iloc[-1]/close.iloc[-6]-1)*100 if len(close)>5 else 0
    if chg1 > 3: direction_score += 10; reasons.append(f"今日涨幅{chg1:.1f}%(动能强)")
    elif chg1 > 0: direction_score += 3
    elif chg1 < -3: direction_score -= 10; reasons.append(f"今日跌幅{abs(chg1):.1f}%(动能弱)")
    if chg5 > 0: direction_score += 7
    else: direction_score -= 7

    # 5. 价格位置 (占15分)
    ma5v = float(ma(close,5).iloc[-1]); ma20v = float(ma(close,20).iloc[-1])
    if cur > ma5v > ma20v: direction_score += 8
    elif cur > ma20v: direction_score += 3
    elif cur < ma20v: direction_score -= 5

    # 归一化到概率
    # direction_score范围约[-60, +60]，映射到涨跌概率
    if direction_score >= 20: up_prob = 65; down_prob = 35; bias = '偏多'
    elif direction_score >= 5: up_prob = 55; down_prob = 45; bias = '略偏多'
    elif direction_score >= -5: up_prob = 50; down_prob = 50; bias = '方向不明'
    elif direction_score >= -20: up_prob = 45; down_prob = 55; bias = '略偏空'
    else: up_prob = 35; down_prob = 65; bias = '偏空'

    # 基于概率的置信度
    confidence = min(abs(direction_score) * 1.2, 90)

    # === 三种情景预测 ===
    # 基础波动: 0.7倍ATR (大概率区间)
    base_range = atr * 0.7 / cur * 100
    # 正常波动: 1.2倍ATR
    normal_range = atr * 1.2 / cur * 100

    if up_prob >= down_prob:
        # 偏多情景
        bullish_change = round(normal_range * 1.5, 2)
        base_change = round(base_range * 0.6, 2)
        bearish_change = round(-base_range * 0.8, 2)
    else:
        bullish_change = round(base_range * 0.8, 2)
        base_change = round(-base_range * 0.6, 2)
        bearish_change = round(-normal_range * 1.5, 2)

    # 大概率的明日区间
    expected_high = round(cur * (1 + max(bullish_change, base_change) / 100), 2)
    expected_low = round(cur * (1 + min(bearish_change, base_change) / 100), 2)

    return {
        'current_price': cur,
        'atr': round(atr, 2), 'atr_pct': atr_pct,
        'direction_score': direction_score,
        'direction_bias': bias,
        'up_probability': up_prob, 'down_probability': down_prob,
        'confidence': round(confidence, 1),
        'reasons': reasons[:6],
        'expected_range': f"{expected_low} ~ {expected_high}",
        'expected_range_pct': f"{round((expected_low/cur-1)*100,1)}% ~ +{round((expected_high/cur-1)*100,1)}%",
        'scenarios': [
            {'name': '乐观情景', 'probability': f'{up_prob}%',
             'change': f'+{bullish_change}%' if bullish_change>0 else f'{bullish_change}%',
             'price': round(cur*(1+bullish_change/100), 2),
             'desc': '量能配合、突破阻力位时可能达到'},
            {'name': '基准情景', 'probability': '最大可能',
             'change': f'{base_change:+.1f}%',
             'price': round(cur*(1+base_change/100), 2),
             'desc': '正常波动范围内最可能的走势'},
            {'name': '悲观情景', 'probability': f'{down_prob}%',
             'change': f'{bearish_change}%' if bearish_change>0 else f'{bearish_change}%',
             'price': round(cur*(1+bearish_change/100), 2),
             'desc': '遇到抛压或利空时可能回撤至此'},
        ],
        'summary': (f"基于ATR({atr:.2f})和多因子分析，预计明日大概率波动区间为{expected_low}~{expected_high}"
                    f"({round((expected_low/cur-1)*100,1)}%~+{round((expected_high/cur-1)*100,1)}%)。"
                    f"方向偏向: {bias}(置信度{confidence:.0f}%)。"
                    f"涨跌概率: 上涨{up_prob}% vs 下跌{down_prob}%。")
    }

def analyze_t0_trading(df, indicators, cur_time=None):
    """做T分析：盘中=当日做T，盘后=次日做T计划"""
    close = df['close']; high = df['high']; low = df['low']
    cur = float(close.iloc[-1])

    # 判断当前时段
    now = cur_time or datetime.now()
    is_trading = False
    if now.weekday() < 5:
        t = now.hour * 60 + now.minute
        if (570 <= t <= 690) or (780 <= t <= 900):  # 9:30-11:30 or 13:00-15:00
            is_trading = True

    if is_trading:
        # === 盘中：当日做T ===
        # 用昨日OHLC做枢轴
        mode = 'intraday'
        ref_h = float(high.iloc[-2]); ref_l = float(low.iloc[-2]); ref_c = float(close.iloc[-2])
        ref_label = '昨日'
        pivot = round((ref_h + ref_l + ref_c) / 3, 2)
        r1 = round(2*pivot - ref_l, 2); r2 = round(pivot + (ref_h - ref_l), 2)
        s1 = round(2*pivot - ref_h, 2); s2 = round(pivot - (ref_h - ref_l), 2)

        # 当前价格位置判断
        if cur > r1: pos = f"当前价{cur}已突破R1({r1})，强势运行"; pos_cls = 'bullish'
        elif cur > pivot: pos = f"当前价{cur}在枢轴{pivot}上方，偏多"; pos_cls = 'bullish'
        elif cur > s1: pos = f"当前价{cur}在枢轴{pivot}下方，偏弱"; pos_cls = 'bearish'
        elif cur > s2: pos = f"当前价{cur}已跌破S1({s1})，弱势运行"; pos_cls = 'bearish'
        else: pos = f"当前价{cur}跌破S2({s2})，极度弱势"; pos_cls = 'bearish'

        buy_points = [
            {'price': round(s1,2), 'label': 'S1枢轴支撑', 'desc': f'回落至{s1}不破可做T买入'},
            {'price': round(ref_c,2), 'label': '昨日收盘价', 'desc': f'回踩{ref_c}获支撑后买入'},
            {'price': round(s2,2), 'label': 'S2极限支撑', 'desc': f'急跌至{s2}是抄底做T良机'},
        ]
        sell_points = [
            {'price': round(r1,2), 'label': 'R1枢轴阻力', 'desc': f'反弹至{r1}无力突破则卖出'},
            {'price': round(ref_h,2), 'label': '昨日最高点', 'desc': f'触及{ref_h}附近减仓'},
            {'price': round(r2,2), 'label': 'R2强阻力', 'desc': f'冲至{r2}大概率回落，做T清仓'},
        ]

        t0_advice = (f"【盘中做T·当日】{pos}。"
                     f"做T买入参考S1({s1})，卖出参考R1({r1})。"
                     f"若跌破S2({s2})止损，突破R2({r2})可追多。注意14:30后减少操作以防尾盘波动。")

    else:
        # === 盘后：次日做T计划 ===
        mode = 'next_day'
        # 用今日OHLC计算明日枢轴
        ref_h = float(high.iloc[-1]); ref_l = float(low.iloc[-1]); ref_c = float(close.iloc[-1])
        ref_label = '今日'
        pivot = round((ref_h + ref_l + ref_c) / 3, 2)
        r1 = round(2*pivot - ref_l, 2); r2 = round(pivot + (ref_h - ref_l), 2)
        s1 = round(2*pivot - ref_h, 2); s2 = round(pivot - (ref_h - ref_l), 2)

        buy_points = [
            {'price': round(s1,2), 'label': 'S1明日支撑', 'desc': f'明日若回踩{s1}不破可做T买入'},
            {'price': round(ref_c,2), 'label': '今日收盘价', 'desc': f'回踩{ref_c}企稳后可买'},
            {'price': round(s2,2), 'label': 'S2强支撑', 'desc': f'若急跌至{s2}是次日做T好买点'},
        ]
        sell_points = [
            {'price': round(r1,2), 'label': 'R1明日阻力', 'desc': f'明日冲至{r1}若量能不济则卖'},
            {'price': round(ref_h,2), 'label': '今日最高点', 'desc': f'触及今日高点{ref_h}附近减仓'},
            {'price': round(r2,2), 'label': 'R2强阻力', 'desc': f'冲至{r2}大概率回落，做T离场'},
        ]

        t0_advice = (f"【次日做T计划】基于{ref_label}数据计算明日枢轴={pivot}。"
                     f"明日若开盘在枢轴上方则偏多，回踩S1({s1})买入、冲高R1({r1})卖出。"
                     f"若开盘在枢轴下方则偏空，反弹R1({r1})减仓、急跌S2({s2})抄底。止损设于S2({s2})下方。")

    # ATR
    tr = pd.DataFrame({'hl':high-low,'hpc':abs(high-close.shift(1)),'lpc':abs(low-close.shift(1))}).max(axis=1)
    atr = float(tr.tail(14).mean())
    atr_pct = round(atr/cur*100, 2)

    # 振幅
    amps = [(float(high.iloc[i])/float(low.iloc[i])-1)*100 for i in range(-6, -1) if float(low.iloc[i])>0]
    avg_amp = round(np.mean(amps), 2) if amps else 3.0

    # 做T空间
    t0_buy = s1 if s1 < cur else (buy_points[0]['price'] if buy_points else cur)
    t0_sell = r1 if r1 > cur else (sell_points[0]['price'] if sell_points else cur)
    t0_space = round((t0_sell/t0_buy - 1)*100, 2) if t0_buy > 0 and t0_sell > t0_buy else round(avg_amp*0.4, 2)
    t0_risk = round(atr_pct * 0.5, 2)

    return {
        'mode': mode, 'is_trading': is_trading,
        'pivot': pivot, 'r1': r1, 'r2': r2, 's1': s1, 's2': s2,
        'ref_label': ref_label, 'avg_amplitude': avg_amp, 'atr_pct': atr_pct,
        'buy_points': buy_points, 'sell_points': sell_points,
        't0_space': t0_space, 't0_risk': t0_risk,
        'advice': t0_advice,
    }

def plan_route_to_target(buy_price, shares, cur_price, target_amount, supports, resistances, atr_pct):
    """制定达到目标盈利的逐节点操作路线图"""
    cur = cur_price; cost = buy_price * shares
    total_target = cost + target_amount  # 需要达到的总市值
    target_per_share = total_target / shares  # 简单目标价
    nodes = []

    # 收集关键价位并排序
    key_levels = set()
    key_levels.add(cur)  # 当前价
    key_levels.add(round(target_per_share, 2))  # 目标价
    for s in supports: key_levels.add(s['level'])
    for r in resistances: key_levels.add(r['level'])
    # 加一些中间节点
    for pct in [0.95, 0.97, 1.03, 1.05, 1.08, 1.10, 1.15, 1.20]:
        key_levels.add(round(cur * pct, 2))
    key_levels = sorted([x for x in key_levels if x > 0])

    # 过滤: 只在cur的下方和上方各取3-5个关键节点
    below = [x for x in key_levels if x < cur * 0.99][-4:]
    above = [x for x in key_levels if x > cur * 1.01][:5]
    all_nodes = below + [cur] + above

    # 为每个节点制定操作
    current_shares = shares; current_avg = buy_price
    for node_price in all_nodes:
        if abs(node_price - cur) < 0.01: continue  # 跳过当前价

        if node_price < cur:
            # 价格下跌到支撑位→考虑加仓
            add_ratio = 0.3 if node_price < cur * 0.95 else 0.2 if node_price < cur * 0.97 else 0.1
            add_shares = int(current_shares * add_ratio)
            if add_shares < 100: add_shares = 100
            new_avg = round((current_avg * current_shares + node_price * add_shares) / (current_shares + add_shares), 2)
            new_total = current_shares + add_shares
            new_target = round(total_target / new_total, 2)

            desc = (f"在{node_price}元补仓{add_shares}股(约{add_shares*node_price:.0f}元)，"
                    f"均价从{current_avg}降至{new_avg}。补仓后需涨至{new_target}元完成目标。")
            time_note = f"距当前{-round((node_price/cur-1)*100,1)}%，按波动率约需{max(1,int(abs(node_price-cur)/cur/atr_pct*100))}个交易日到达"

            nodes.append({'trigger': '下跌至', 'price': node_price, 'action': '加仓',
                          'shares': f'+{add_shares}', 'total_after': new_total,
                          'avg_after': new_avg, 'target_after': new_target,
                          'desc': desc, 'time': time_note, 'direction': 'down'})
        else:
            # 价格上涨→考虑减仓/止盈
            sell_ratio = 0.2 if node_price < target_per_share * 0.5 else 0.4 if node_price < target_per_share else 0.7
            sell_shares = int(current_shares * sell_ratio)
            if sell_shares < 100: sell_shares = min(100, current_shares)
            if sell_shares >= current_shares: sell_shares = current_shares
            profit_locked = round((node_price - current_avg) * sell_shares, 2)
            remaining = current_shares - sell_shares
            remaining_needed = total_target - profit_locked
            remaining_target = round(remaining_needed / remaining, 2) if remaining > 0 else 0
            progress = round(profit_locked / target_amount * 100, 1) if target_amount > 0 else 0

            if remaining <= 0:
                desc = f"在{node_price}元全部清仓，获利{profit_locked:.0f}元。"
            else:
                desc = (f"在{node_price}元卖出{sell_shares}股锁定{profit_locked:.0f}元利润(完成{progress}%)，"
                        f"剩余{remaining}股需涨至{remaining_target}元完成目标。")
            time_note = f"距当前+{round((node_price/cur-1)*100,1)}%，约需{max(1,int(abs(node_price-cur)/cur/atr_pct*100))}个交易日"

            nodes.append({'trigger': '上涨至', 'price': node_price, 'action': '减仓止盈',
                          'shares': f'-{sell_shares}', 'total_after': remaining,
                          'avg_after': current_avg, 'profit_locked': profit_locked,
                          'progress_pct': progress, 'remaining_target': remaining_target,
                          'desc': desc, 'time': time_note, 'direction': 'up'})

    # 按价格从小到大排序(从低价补仓到高价止盈)
    nodes.sort(key=lambda x: x['price'])

    # 汇总
    summary = (f"当前持仓{shares}股@成本{buy_price}元，目标盈利{target_amount:.0f}元(总市值{total_target:.0f}元)。"
               f"以下为逐节点操作路线，共{len(nodes)}个关键节点。")

    return {'nodes': nodes, 'summary': summary, 'target_amount': target_amount,
            'total_target': round(total_target, 2), 'simple_target_price': round(target_per_share, 2)}

def analyze_position(code, market, buy_price, shares, cur_price, indicators, target_amount=None):
    """持仓量化分析：加仓/减仓/回本方案 + 时间预估 + 目标路线图"""
    buy_price = float(buy_price); shares = int(shares); cur = float(cur_price)
    cost = buy_price * shares
    value = cur * shares
    pnl = value - cost
    pnl_pct = round((cur/buy_price - 1)*100, 2)
    atr_pct = round(float(indicators.get('macd_bar', 0))*0 + 2.5, 1)  # fallback 2.5%

    # 计算ATR
    rsi_v = indicators.get('rsi', 50)
    ma20_v = indicators.get('ma20', cur)
    ma60_v = indicators.get('ma60', cur)

    # 日预期波动(基准1.8%，根据RSI偏离调整，范围1.5%-2.8%)
    daily_move = 1.8 + abs(rsi_v - 50) * 0.02

    result = {
        'code': code, 'market': market,
        'buy_price': buy_price, 'shares': shares, 'cost': round(cost, 2),
        'current_price': cur, 'current_value': round(value, 2),
        'pnl': round(pnl, 2), 'pnl_pct': pnl_pct,
        'strategies': []
    }

    # === 策略1: 目标盈利方案 ===
    # 使用用户自定义目标，否则默认10%, 20%, 30%
    target_pcts = [10, 20, 30]
    if target_amount:
        custom_pct = round(target_amount / cost * 100, 1)
        target_pcts = [custom_pct] + [p for p in target_pcts if abs(p - custom_pct) > 3]

    for target_pct in target_pcts:
        target_price = round(buy_price * (1 + target_pct/100), 2)
        target_value = target_price * shares
        target_profit = round(target_value - cost, 2)
        gap = target_price - cur
        gap_pct = round((target_price/cur - 1)*100, 2)
        if gap > 0:
            est_days = round(gap_pct / daily_move, 0) if daily_move > 0 else 30
        else:
            est_days = 0
        is_custom = target_amount and abs(target_pct - custom_pct) < 0.5
        result['strategies'].append({
            'type': '目标盈利', 'target': f'+{target_pct}%' + ('(你的目标)' if is_custom else ''),
            'target_price': target_price, 'target_profit': target_profit,
            'gap_pct': gap_pct, 'est_days': int(est_days),
            'action': f"持有至{target_price}元后卖出{shares}股，获利{target_profit:.0f}元",
            'time_note': f"按当前波动率，预计需{int(est_days)}个交易日" if est_days>0 else "当前已达标"
        })

    # === 策略2: 加仓摊低成本(适用于亏损) ===
    if pnl < 0:
        # 目标: 加仓使平均成本降至当前价上方不远处
        # 新平均成本 = (cost + add_shares*cur) / (shares + add_shares)
        # 要降到target_avg: add_shares = (shares*buy_price - target_avg*shares) / (target_avg - cur)
        for ratio, label in [(0.5, '减亏一半'), (1.0, '拉平成本'), (2.0, '大幅摊薄')]:
            add_shares = int(shares * ratio)
            new_avg = round((cost + add_shares*cur) / (shares + add_shares), 2)
            new_total = shares + add_shares
            new_cost = round(cost + add_shares*cur, 2)
            # 新回本价 = 新平均成本
            breakeven_pct = round((new_avg/cur - 1)*100, 2)
            result['strategies'].append({
                'type': '加仓摊薄', 'label': label,
                'add_shares': add_shares, 'add_amount': round(add_shares*cur, 2),
                'new_avg_cost': new_avg, 'new_total_shares': new_total,
                'new_total_cost': new_cost,
                'breakeven_price': new_avg,
                'breakeven_pct': breakeven_pct,
                'action': f"以当前价{cur}元买入{add_shares}股(约{add_shares*cur:.0f}元)，平均成本降至{new_avg}元。回本需涨至{new_avg}元(需涨{breakeven_pct}%)",
                'time_note': f"按当前波动率，回本约需{max(1,int(breakeven_pct/daily_move))}个交易日" if breakeven_pct>0 else "已回本"
            })

    # === 策略3: 分批止盈(适用于盈利) ===
    if pnl > 0:
        for sell_ratio, label in [(0.5, '半仓止盈'), (1.0, '全部止盈')]:
            sell_shares = int(shares * sell_ratio)
            sell_price = cur
            profit = round((sell_price - buy_price) * sell_shares, 2)
            remain = shares - sell_shares
            result['strategies'].append({
                'type': '止盈', 'label': label,
                'sell_shares': sell_shares, 'sell_price': cur,
                'profit_locked': profit, 'remain_shares': remain,
                'action': f"以当前价{cur}卖出{sell_shares}股锁定利润{profit:.0f}元，剩余{remain}股零成本持有",
                'time_note': "随时可执行"
            })

    # === 策略4: 止损方案 ===
    stop_price = round(buy_price * 0.93, 2)
    stop_loss_val = round((stop_price - buy_price) * shares, 2)
    if cur > stop_price:
        stop_note = f'当前距止损位还有{round((cur/stop_price-1)*100,1)}%空间'
    else:
        stop_note = f'当前价已跌破止损位！建议立即止损'
    result['strategies'].append({
        'type': '止损', 'label': '硬止损(-7%)',
        'stop_price': stop_price, 'stop_loss': stop_loss_val,
        'action': f"若跌破{stop_price}元(亏损{stop_loss_val:.0f}元)，建议无条件止损离场",
        'time_note': stop_note
    })

    # 综合建议
    if pnl_pct >= 10:
        result['advice'] = f"当前盈利{pnl_pct}%，建议设置移动止盈(如回撤3%即卖)。核心仓位可继续持有看中期目标。"
    elif pnl_pct >= 0:
        result['advice'] = f"当前微盈{pnl_pct}%。建议持有观望，止损设于成本价下方3%。若放量突破可加仓。"
    elif pnl_pct >= -5:
        result['advice'] = f"当前浮亏{abs(pnl_pct)}%，处于浅套状态。不建议盲目加仓，等待企稳信号(放量阳线或MACD金叉)后再考虑操作。"
    elif pnl_pct >= -15:
        result['advice'] = f"当前浮亏{abs(pnl_pct)}%，中度套牢。若仓位不重(<3成)可考虑在MA60({ma60_v})附近加仓摊薄；若仓位重则反弹至MA20({ma20_v})附近减仓。"
    else:
        result['advice'] = f"当前深套{abs(pnl_pct)}%。建议：1)不要在下跌中加仓 2)等待反弹至关键阻力位分批减仓 3)保留资金等待市场企稳后再战。"

    return result

def fetch_eastmoney_f10(code):
    """从东方财富F10页面获取详细信息：简介、主营构成、财务亮点、股东"""
    result = {}
    sec_mkt = 'SH' if code.startswith('6') else 'SZ'
    sec_full = f'{sec_mkt}{code}'

    try:
        import requests
        s = requests.Session(); s.trust_env = False
        base_headers = {'User-Agent': 'Mozilla/5.0', 'Referer': 'https://emweb.securities.eastmoney.com/'}
        no_proxy = {'http': None, 'https': None}

        # --- 1. 公司概况 ---
        try:
            resp = s.get('https://emweb.securities.eastmoney.com/PC_HSF10/CompanySurvey/CompanySurveyAjax',
                         params={'code': sec_full}, timeout=3, headers=base_headers, proxies=no_proxy)
            if resp.status_code == 200:
                data = resp.json() if resp.text else {}
                if isinstance(data, dict):
                    # 递归提取所有文本
                    def extract_text(obj, depth=0):
                        if depth > 3: return
                        if isinstance(obj, dict):
                            # 常见字段直接匹配
                            field_map = {
                                'gsjj': 'company_intro', 'gsgk': 'company_intro',
                                'zyyw': 'main_business', 'mainBusiness': 'main_business',
                                'gsmc': 'full_name', 'compName': 'full_name',
                                'frdb': 'legal_person', 'legal_repr': 'legal_person',
                                'clrq': 'establish_date', 'establishment_date': 'establish_date',
                                'ssrq': 'ipo_date_em', 'listedDate': 'ipo_date_em',
                                'zcdz': 'reg_addr', 'reg_addr': 'reg_addr',
                                'website': 'website', 'gsweb': 'website',
                                'ygrs': 'employees', 'employeeNum': 'employees',
                                'zgb': 'total_shares_raw', 'totalShares': 'total_shares_raw',
                                'ltgb': 'circulation_shares_raw',
                                'compProfile': 'company_intro',
                                'businessScope': 'main_business',
                                'compIntro': 'company_intro',
                            }
                            for k, v in obj.items():
                                if k in field_map and v:
                                    key = field_map[k]
                                    if key not in result or not result[key]:
                                        if isinstance(v, str) and len(v) > 2:
                                            result[key] = str(v)[:800]
                            # 递归子对象
                            for v in obj.values():
                                if isinstance(v, (dict, list)):
                                    extract_text(v, depth+1)
                    extract_text(data)
        except: pass

        # --- 2. 主营构成 ---
        try:
            resp = s.get('https://emweb.securities.eastmoney.com/PC_HSF10/BusinessAnalysis/BusinessAnalysisAjax',
                         params={'code': sec_full}, timeout=3, headers=base_headers, proxies=no_proxy)
            if resp.status_code == 200:
                data = resp.json() if resp.text else {}
                if isinstance(data, dict):
                    biz_parts = []
                    for key in ['zyyw', 'mainBusiness', 'businessScope']:
                        v = _deep_find(data, key)
                        if v and isinstance(v, str) and len(v) > 10:
                            biz_parts.append(str(v)[:400])
                            break
                    # 查找产品列表
                    for arr_key in ['productList', 'businessList', 'yygclist', 'products']:
                        arr = _deep_find(data, arr_key)
                        if arr and isinstance(arr, list) and len(arr) > 0:
                            items = []
                            for item in arr[:5]:
                                if isinstance(item, dict):
                                    name = item.get('productName') or item.get('name') or item.get('type') or ''
                                    ratio = item.get('ratio') or item.get('percent') or item.get('rate') or ''
                                    if name:
                                        items.append(f"{name}({ratio})" if ratio else name)
                            if items: biz_parts.append('主营构成: ' + '、'.join(items))
                    if biz_parts: result['business_detail'] = '；'.join(biz_parts)
        except: pass

        # --- 3. 财务亮点 ---
        try:
            resp = s.get('https://emweb.securities.eastmoney.com/PC_HSF10/FinanceSummary/FinanceSummaryAjax',
                         params={'code': sec_full}, timeout=3, headers=base_headers, proxies=no_proxy)
            if resp.status_code == 200:
                data = resp.json() if resp.text else {}
                if isinstance(data, dict):
                    fin_info = {}
                    for k, v in data.items():
                        if isinstance(v, (int, float, str)) and v:
                            fin_info[k] = str(v)[:200]
                    if fin_info: result['financial_extra'] = fin_info
        except: pass

        # --- 处理股本数据 ---
        if result.get('total_shares_raw'):
            try:
                v = float(result['total_shares_raw'])
                result['total_shares'] = f"{v/100000000:.2f}亿股" if v>1e7 else f"{v/10000:.0f}万股"
            except: pass
        if result.get('circulation_shares_raw'):
            try:
                v = float(result['circulation_shares_raw'])
                result['circulation_shares'] = f"{v/100000000:.2f}亿股" if v>1e7 else f"{v/10000:.0f}万股"
            except: pass

    except:
        pass

    return result

def _deep_find(obj, target_key):
    """递归查找嵌套字典中的键"""
    if isinstance(obj, dict):
        if target_key in obj: return obj[target_key]
        for v in obj.values():
            r = _deep_find(v, target_key)
            if r is not None: return r
    elif isinstance(obj, list):
        for item in obj:
            r = _deep_find(item, target_key)
            if r is not None: return r
    return None

def fetch_fundamental_data(code, market='A'):
    """获取详细基本面数据：公司概况、行业、财务、估值"""
    result = {
        'company_name': '', 'industry': '', 'industry_detail': '',
        'main_business': '', 'market_cap': '', 'pe': '', 'pb': '',
        'revenue': '', 'revenue_growth': '', 'net_profit': '', 'profit_growth': '',
        'eps': '', 'roe': '', 'net_margin': '', 'gross_margin': '',
        'debt_ratio': '', 'total_assets': '', 'holders': '',
        'summary': '', 'risk_note': ''
    }

    if market != 'A':
        result['summary'] = '港股基本面数据需通过东方财富F10页面查阅'
        return result

    ts_code = f'{code}.SH' if code.startswith('6') else f'{code}.SZ'

    # === Tushare: 公司信息 + 财务指标 ===
    if TUSHARE_AVAILABLE:
        try:
            # 公司基本信息
            company = tushare_pro.stock_company(ts_code=ts_code,
                fields='ts_code,chairman,manager,reg_capital,setup_date,province,city,introduction,website,employees,main_business,business_scope')
            if company is not None and len(company) > 0:
                row = company.iloc[0]
                result['main_business'] = str(row.get('main_business', '') or '')
                result['company_intro'] = str(row.get('introduction', '') or '')[:800]
                result['employees'] = str(row.get('employees', '') or '')
                result['reg_addr'] = f"{row.get('province', '')}{row.get('city', '')}"
                result['website'] = str(row.get('website', '') or '')

            # 财务指标
            indicator = tushare_pro.fina_indicator(ts_code=ts_code,
                fields='ts_code,ann_date,roe,grossprofit_margin,netprofit_margin,debt_to_assets,eps,dt_eps,total_revenue,revenue')
            if indicator is not None and len(indicator) > 0:
                latest = indicator.iloc[0]
                if latest.get('roe'): result['roe'] = f"{float(latest['roe']):.2f}%"
                if latest.get('grossprofit_margin'): result['gross_margin'] = f"{float(latest['grossprofit_margin']):.2f}%"
                if latest.get('netprofit_margin'): result['net_margin'] = f"{float(latest['netprofit_margin']):.2f}%"
                if latest.get('debt_to_assets'): result['debt_ratio'] = f"{float(latest['debt_to_assets']):.2f}%"
                if latest.get('eps'): result['eps'] = f"{float(latest['eps']):.4f}元"
                if latest.get('total_revenue'): result['revenue'] = f"{float(latest['total_revenue'])/100000000:.2f}亿"

            # 行业分类
            try:
                industry = tushare_pro.stock_basic(ts_code=ts_code, fields='ts_code,industry')
                if industry is not None and len(industry) > 0:
                    result['industry'] = str(industry.iloc[0].get('industry', '') or '')
            except: pass

            # 公司名称
            try:
                name_df = tushare_pro.stock_basic(ts_code=ts_code, fields='ts_code,name')
                if name_df is not None and len(name_df) > 0:
                    result['company_name'] = str(name_df.iloc[0].get('name', '') or '')
            except: pass

            log(f"基本面数据(Tushare): {code} {result['company_name']}")
        except Exception as e:
            log(f"Tushare基本面获取失败，降级到baostock: {e}")

    # === 降级到baostock ===
    bs_code = f'sh.{code}' if code.startswith('6') else f'sz.{code}'

    if not result['company_name']:
        try:
            rs = bs.query_stock_basic(bs_code)
            if rs.error_code == '0':
                while rs.next():
                    row = rs.get_row_data()
                    result['company_name'] = row[1]
                    result['ipo_date'] = row[2]
                    break
        except: pass

    if not result['industry']:
        try:
            rs = bs.query_stock_industry(bs_code)
            if rs.error_code == '0':
                inds = []
                while rs.next():
                    row = rs.get_row_data()
                    if len(row) >= 5:
                        inds.append({'code': row[1], 'name': row[3], 'type': row[4]})
                if inds:
                    result['industry'] = inds[-1]['name']
                    result['industry_detail'] = ' → '.join([i['name'] for i in inds])
        except: pass

    # === 3. 财务数据(最近3年年报，保留趋势对比) ===
    year = datetime.now().year
    annual_data = {}

    for y in range(year-1, year-4, -1):
        yr_data = {}
        try:
            rs = bs.query_profit_data(bs_code, year=y, quarter=4)
            if rs.error_code == '0':
                d = rs.get_data()
                if d is not None and len(d) > 0:
                    row = d.iloc[-1]
                    for k in ['营业收入','operRev','营业总收入']:
                        if k in row.index and row[k]:
                            try: yr_data['revenue'] = float(row[k])/100000000; break
                            except: pass
                    for k in ['归属母公司股东净利润','parentNetProfit']:
                        if k in row.index and row[k]:
                            try: yr_data['profit'] = float(row[k])/100000000; break
                            except: pass
        except: pass
        try:
            rs = bs.query_operation_data(bs_code, year=y, quarter=4)
            if rs.error_code == '0':
                d = rs.get_data()
                if d is not None and len(d) > 0:
                    row = d.iloc[-1]
                    for k in ['净资产收益率','ROE']:
                        if k in row.index and row[k]:
                            try: yr_data['roe'] = float(row[k]); break
                            except: pass
                    for k in ['每股收益','EPS','基本每股收益']:
                        if k in row.index and row[k]:
                            try: yr_data['eps'] = float(row[k]); break
                            except: pass
                    for k in ['销售毛利率','GPM','毛利率']:
                        if k in row.index and row[k]:
                            try: yr_data['gross_margin'] = float(row[k]); break
                            except: pass
        except: pass
        if yr_data: annual_data[y] = yr_data

    # 填充最新值和同比
    years_sorted = sorted(annual_data.keys(), reverse=True)
    if years_sorted:
        latest = annual_data[years_sorted[0]]
        if latest.get('revenue'): result['revenue'] = f"{latest['revenue']:.2f}亿"
        if latest.get('profit'): result['net_profit'] = f"{latest['profit']:.2f}亿"
        if latest.get('eps'): result['eps'] = f"{latest['eps']:.4f}元"
        if latest.get('roe'): result['roe'] = f"{latest['roe']:.2f}%"
        if latest.get('gross_margin'): result['gross_margin'] = f"{latest['gross_margin']:.2f}%"
        if len(years_sorted) >= 2:
            prev = annual_data[years_sorted[1]]
            if latest.get('revenue') and prev.get('revenue') and prev['revenue'] > 0:
                result['revenue_growth'] = f"{(latest['revenue']/prev['revenue']-1)*100:+.1f}%"
            if latest.get('profit') and prev.get('profit') and prev['profit'] > 0:
                result['profit_growth'] = f"{(latest['profit']/prev['profit']-1)*100:+.1f}%"
        # 5年趋势数据存入
        result['annual_trend'] = annual_data

    # 资产负债表(快速, 仅取最新)
    try:
        rs = bs.query_balance_data(bs_code, year=year-1, quarter=4)
        if rs.error_code == '0':
            d = rs.get_data()
            if d is not None and len(d) > 0:
                b = d.iloc[-1]
                for k in ['资产总计','totalAssets']:
                    if k in b.index and b[k]:
                        try: result['total_assets'] = f"{float(b[k])/100000000:.2f}亿"; break
                        except: pass
                for ak in ['资产总计','totalAssets']:
                    for lk in ['负债合计','totalLiabilities']:
                        if ak in b.index and lk in b.index and b[ak] and b[lk]:
                            try: result['debt_ratio'] = f"{float(b[lk])/float(b[ak])*100:.1f}%"; break
                            except: pass
    except: pass

    # === 4. 联网获取F10详细信息 ===
    f10 = fetch_eastmoney_f10(code)
    if f10:
        for k in ['company_intro','main_business','full_name','legal_person',
                  'establish_date','reg_addr','website','employees','total_shares','circulation_shares']:
            if f10.get(k) and not result.get(k): result[k] = f10[k]

    # === 5. 生成研报级概述 ===
    name = result.get('company_name', code)
    ind = result.get('industry', '')
    biz = result.get('main_business', '')
    intro = result.get('company_intro', '')
    loc = result.get('reg_addr', '')
    full_nm = result.get('full_name', name)

    # 段落1: 公司定位
    lines = []
    loc_str = f"，位于{loc[:20]}" if loc else ""
    ind_str = f"，所属{ind}行业" if ind else ""
    lines.append(f"【公司定位】{full_nm}（{code}），{result.get('ipo_date','')}上市{loc_str}{ind_str}。")

    # 段落2: 主营业务
    if biz or intro:
        desc = biz or (intro[:200] if intro else '')
        lines.append(f"【主营业务】{desc}")

    # 段落3: 行业地位
    if ind:
        industry_chain = result.get('industry_detail', ind)
        lines.append(f"【行业分类】{industry_chain}")
        if result.get('total_shares'): lines.append(f"【股本结构】总股本{result['total_shares']}" + (f"，流通{result['circulation_shares']}" if result.get('circulation_shares') else ""))

    # 段落4: 近年财务趋势
    if annual_data:
        lines.append(f"【近年财务趋势】")
        for yr in sorted(annual_data.keys()):
            d = annual_data[yr]
            rev_str = f"营收{d['revenue']:.1f}亿" if d.get('revenue') else ''
            np_str = f"净利{d['profit']:.1f}亿" if d.get('profit') else ''
            roe_str = f"ROE{d['roe']:.1f}%" if d.get('roe') else ''
            parts_fin = [x for x in [rev_str, np_str, roe_str] if x]
            if parts_fin:
                tag = '📈' if d.get('profit',0) > annual_data.get(yr+1,{}).get('profit', d['profit']+1) else '📉' if d.get('profit',0) < annual_data.get(yr+1,{}).get('profit', d['profit']-1) else '➡️'
                lines.append(f"  {yr}年 {tag} {' | '.join(parts_fin)}")

    # 段落5: 最新财务亮点
    fin_highlights = []
    if result.get('revenue'): fin_highlights.append(f"营收{result['revenue']}")
    if result.get('revenue_growth'): fin_highlights.append(f"同比{result['revenue_growth']}")
    if result.get('net_profit'): fin_highlights.append(f"净利{result['net_profit']}")
    if result.get('profit_growth'): fin_highlights.append(f"利润{result['profit_growth']}")
    if result.get('roe'): fin_highlights.append(f"ROE{result['roe']}")
    if result.get('eps'): fin_highlights.append(f"EPS{result['eps']}")
    if result.get('gross_margin'): fin_highlights.append(f"毛利率{result['gross_margin']}")
    if fin_highlights: lines.append(f"【最新财务】{' | '.join(fin_highlights)}")

    # 段落6: 财务健康度
    health = []
    if result.get('debt_ratio'): health.append(f"资产负债率{result['debt_ratio']}")
    if result.get('total_assets'): health.append(f"总资产{result['total_assets']}")
    if result.get('employees'): health.append(f"员工{result['employees']}人")
    if health: lines.append(f"【财务健康】{' | '.join(health)}")

    # 段落7: 优势与风险
    strengths = []; risks = []
    try:
        if result.get('profit_growth') and not result['profit_growth'].startswith('-') and float(result['profit_growth'].replace('%','').replace('+','')) > 10:
            strengths.append("利润增速>10%，成长性良好")
    except: pass
    try:
        if result.get('roe') and float(result['roe'].replace('%','')) > 15:
            strengths.append(f"ROE>15%，盈利能力优秀")
    except: pass
    try:
        if result.get('debt_ratio') and float(result['debt_ratio'].replace('%','')) < 40:
            strengths.append("负债率低，财务稳健")
        elif result.get('debt_ratio') and float(result['debt_ratio'].replace('%','')) > 60:
            risks.append(f"负债率偏高({result['debt_ratio']})，财务杠杆大")
    except: pass
    try:
        if result.get('gross_margin') and float(result['gross_margin'].replace('%','')) > 30:
            strengths.append(f"毛利率{result['gross_margin']}，产品竞争力强")
    except: pass
    if result.get('profit_growth') and result['profit_growth'].startswith('-'):
        risks.append(f"利润{result['profit_growth']}，盈利下滑需警惕")
    if not strengths: strengths.append("暂未识别出显著竞争优势(数据有限)")
    if not risks: risks.append("暂未发现明显财务风险信号")
    lines.append(f"【优势】{'；'.join(strengths)}")
    lines.append(f"【风险】{'；'.join(risks)}")

    result['summary'] = '\n'.join(lines)
    result['risk_note'] = '；'.join(risks)

    return result

def _quick_fetch_peer_price(code_str):
    """快速获取一只股票的价格和涨跌"""
    try:
        bs_c = f'sh.{code_str}' if code_str.startswith('6') else f'sz.{code_str}'
        ed = datetime.now().strftime('%Y-%m-%d')
        sd = (datetime.now() - timedelta(days=80)).strftime('%Y-%m-%d')
        rs = bs.query_history_k_data_plus(bs_c, 'date,close,volume', start_date=sd, end_date=ed, frequency='d', adjustflag='2')
        if rs.error_code != '0': return None
        d = rs.get_data()
        if d is None or len(d) < 5: return None
        c = [float(x) for x in d['close']]
        p, chg5, chg20, chg60 = c[-1], 0, 0, 0
        if len(c) >= 6: chg5 = round((c[-1]/c[-6]-1)*100, 2)
        if len(c) >= 2: chg20 = round((c[-1]/c[0]-1)*100, 2)
        chg60 = round((c[-1]/c[0]-1)*100, 2)
        trend = '-'; vol_level = '-'
        if len(c) >= 20:
            m20 = sum(c[-20:])/20
            trend = '上升' if (p > m20 and chg5 > 0) else ('下降' if p < m20 else '盘整')
        if 'volume' in d.columns and len(d) >= 20:
            vols = [float(x) for x in d['volume']]
            vr = (sum(vols[-5:])/5)/(sum(vols[-20:])/20) if sum(vols[-20:])>0 else 1
            vol_level = '放量' if vr>1.3 else ('缩量' if vr<0.7 else '正常')
        return {'price': round(p,2), 'chg_5d': chg5, 'chg_20d': chg20, 'chg_60d': chg60, 'trend': trend, 'vol_level': vol_level}
    except: return None

def fetch_board_and_peers(code, industry_name):
    """获取股票所属板块指数和同行业可比公司"""
    result = {'boards': [], 'indexes': [], 'peers': []}

    # 1. 指数归属(根据代码判断)
    if code.startswith('300'): result['indexes'].append('创业板指(399006)')
    elif code.startswith('688'): result['indexes'].append('科创50(000688)')
    elif code.startswith(('000','001','002')): result['indexes'].append('深证成指(399001)')
    elif code.startswith('60'): result['indexes'].append('上证指数(000001)')

    # 2. 尝试获取概念板块
    try:
        import requests
        s = requests.Session(); s.trust_env = False
        no_proxy = {'http': None, 'https': None}
        sec_mkt = 'SH' if code.startswith('6') else 'SZ'
        # 尝试获取股票所属概念板块
        resp = s.get('https://push2.eastmoney.com/api/qt/stock/get', params={
            'secid': f'{"1" if code.startswith("6") else "0"}.{code}',
            'fields': 'f100,f101,f102,f103,f104,f105'
        }, timeout=3, headers={'User-Agent':'Mozilla/5.0'}, proxies=no_proxy)
        if resp.status_code == 200:
            d = resp.json()
            if d and d.get('data'):
                # 尝试解析行业字段
                pass
    except: pass

    # 3. 同行业可比公司
    if industry_name:
        # 构建关键词
        ind_clean = industry_name.replace('行业','').replace('制造','').replace('材料','').replace('股份','').replace('科技','').strip()
        all_kws = {ind_clean}
        if len(ind_clean) >= 2: all_kws.add(ind_clean[:2])
        if result.get('industry_detail'):
            for part in result['industry_detail'].split(' → '):
                p = part.replace('行业','').strip()
                if p and len(p) >= 2: all_kws.add(p)
        try:
            stock_list = fetch_a_stock_list()
            peers_found = []
            # 策略1: 股票名称含行业关键词
            for s in stock_list:
                if len(peers_found) >= 6: break
                if s['code'] == code: continue
                if not any(k and k in s.get('name', '') for k in all_kws if k): continue
                metrics = _quick_fetch_peer_price(s['code'])
                if metrics:
                    peers_found.append({'code': s['code'], 'name': s['name'], **metrics})
            # 策略2: 不够则采样验证行业
            if len(peers_found) < 3:
                sample = [s for s in stock_list[:120] if s['code'] != code and not any(p['code']==s['code'] for p in peers_found)]
                for s in sample:
                    if len(peers_found) >= 6: break
                    try:
                        sub_bs = f'sh.{s["code"]}' if s['code'].startswith('6') else f'sz.{s["code"]}'
                        rs = bs.query_stock_industry(sub_bs)
                        if rs.error_code == '0':
                            while rs.next():
                                r2 = rs.get_row_data()
                                if len(r2) >= 5 and any(kw and len(kw)>=2 and kw in str(r2[3]) for kw in all_kws if kw):
                                    metrics = _quick_fetch_peer_price(s['code'])
                                    if metrics:
                                        peers_found.append({'code': s['code'], 'name': s['name'], **metrics})
                                break
                    except: pass
            if peers_found:
                result['peers'] = peers_found
                result['industry_note'] = f"以下{len(peers_found)}家公司同属「{industry_name}」行业，附多维对比"
        except: pass

    return result

def analyze_market_situation(df, indicators, fundamental):
    """时局分析：结合技术面+基本面判断当前所处阶段"""
    close = df['close']; cur = float(close.iloc[-1])
    ma60 = indicators.get('ma60', cur); ma20 = indicators.get('ma20', cur)
    rsi_v = indicators.get('rsi', 50)
    chg20 = round((close.iloc[-1]/close.iloc[-21]-1)*100, 2) if len(close)>20 else 0
    chg60 = round((close.iloc[-1]/close.iloc[-61]-1)*100, 2) if len(close)>60 else 0

    dif = indicators.get('macd_dif', 0); dea = indicators.get('macd_dea', 0)

    # 技术阶段判断
    if cur > ma20 > ma60 and dif > dea and dif > 0:
        tech_phase = "上升趋势中，均线多头排列，MACD零轴上金叉运行，处于强势阶段"
    elif cur > ma20 and dif > dea:
        tech_phase = "短期反弹中，站上20日均线但中长期均线尚未完全多头，处于修复阶段"
    elif cur < ma60 and dif < 0:
        tech_phase = "下降趋势中，价格低于60日均线，MACD零轴下运行，处于弱势阶段"
    elif cur < ma20 and dif < dea:
        tech_phase = "短期调整中，跌破20日均线，MACD死叉，处于回调阶段"
    else:
        tech_phase = "震荡整理中，多空力量均衡，处于方向选择阶段"

    # 位置判断
    high60 = float(close.tail(60).max()); low60 = float(close.tail(60).min())
    pos_pct = round((cur - low60) / (high60 - low60) * 100, 1) if high60 != low60 else 50
    if pos_pct > 80: position = "处于60日高位区域，追高需谨慎"
    elif pos_pct > 60: position = "处于60日中上部，有一定上行空间但距离阻力不远"
    elif pos_pct > 40: position = "处于60日中枢区域，方向不明确"
    elif pos_pct > 20: position = "处于60日中下部，靠近支撑区域，下行空间有限"
    else: position = "处于60日低位区域，存在超跌反弹机会"

    # 量能判断
    vol_now = float(df['volume'].iloc[-5:].mean()) if len(df['volume'])>5 else 0
    vol_prev = float(df['volume'].iloc[-20:-5].mean()) if len(df['volume'])>20 else 0
    if vol_prev > 0:
        vol_ratio = vol_now / vol_prev
        if vol_ratio > 1.5: vol_phase = "近期显著放量，资金关注度高，筹码交换活跃"
        elif vol_ratio > 1.1: vol_phase = "温和放量，市场参与度正常"
        elif vol_ratio > 0.7: vol_phase = "缩量调整，交投清淡，观望情绪浓厚"
        else: vol_phase = "严重缩量，筹码锁定或无人问津"
    else:
        vol_phase = "量能数据不足"

    # 基本面简要
    fund_note = fundamental.get('summary', '基本面数据待完善')

    # 综合
    summary = (f"【技术面】{tech_phase}。{position}(60日位置{pos_pct}%)。近20日涨幅{chg20}%，近60日涨幅{chg60}%。"
               f"【量能】{vol_phase}。【基本面】{fund_note}。")

    return {
        'tech_phase': tech_phase, 'position_60d': pos_pct, 'position_desc': position,
        'chg_20d': chg20, 'chg_60d': chg60, 'vol_phase': vol_phase,
        'fund_note': fund_note, 'summary': summary
    }

@app.route('/api/analyze/<code>')
def api_analyze_stock(code):
    """单股深度分析"""
    market = request.args.get('market', 'A')
    log(f"单股分析: {code} ({market})")

    try:
        # 获取历史数据
        h = fetch_stock_history(code, market, 120)
        if h is None: return jsonify({'success': False, 'message': f'无法获取{code}的历史数据，请确认代码正确且股票存在'}), 200

        df = pd.DataFrame({'open': h['open'], 'close': h['close'], 'high': h['high'],
                           'low': h['low'], 'volume': h['volume'],
                           'turnover': h.get('turnover', [0]*len(h['close']))})

        close = df['close']; high = df['high']; low = df['low']

        # 计算全部技术指标
        dif, dea, bar = macd(close)
        r = rsi(close)
        kk, dd, jj = kdj(high, low, close)
        ub, mb, lb, bw = boll(close)
        ma5 = ma(close, 5); ma10 = ma(close, 10); ma20 = ma(close, 20); ma60 = ma(close, 60)

        # 当前指标值
        def last_n(s, n=1):
            try: v = s.iloc[-n]; return round(float(v), 4) if not pd.isna(v) else 0
            except: return 0

        indicators = {
            'price': round(float(close.iloc[-1]), 2),
            'change_pct': round((close.iloc[-1]/close.iloc[-2]-1)*100, 2) if len(close)>1 else 0,
            'macd_dif': last_n(dif), 'macd_dea': last_n(dea), 'macd_bar': last_n(bar),
            'rsi': round(last_n(r), 1),
            'kdj_k': round(last_n(kk), 1), 'kdj_d': round(last_n(dd), 1), 'kdj_j': round(last_n(jj), 1),
            'boll_upper': round(last_n(ub), 2), 'boll_mid': round(last_n(mb), 2),
            'boll_lower': round(last_n(lb), 2), 'boll_bandwidth': round(last_n(bw), 2),
            'ma5': round(last_n(ma5), 2), 'ma10': round(last_n(ma10), 2),
            'ma20': round(last_n(ma20), 2), 'ma60': round(last_n(ma60), 2),
        }

        # 检测所有形态
        stock_info = {'code': code, 'name': '', 'price': indicators['price'],
                      'change_pct': indicators['change_pct'],
                      'turnover': float(df['turnover'].iloc[-1]) if 'turnover' in df.columns and len(df['turnover'])>0 else 0}
        result = analyze_stock(stock_info, market)

        # 情绪分析
        sent_score, sent_summary, sent_ind = analyze_sentiment(df, stock_info)

        # 生成投资建议
        patterns = result['patterns'] if result else []
        rec = generate_recommendation(patterns, {'score': sent_score, 'summary': sent_summary}, indicators)

        # 买卖点预测
        pred = predict_targets(df, indicators)

        # 次日走势预测
        next_day = predict_next_day(df, indicators)

        # 日内做T分析
        t0 = analyze_t0_trading(df, indicators)

        # 基本面分析
        fundamental = fetch_fundamental_data(code, market)

        # 板块归属+同类股票
        board_info = fetch_board_and_peers(code, fundamental.get('industry', ''))

        # 时局分析
        situation = analyze_market_situation(df, indicators, fundamental)

        # 查找股票名称
        stock_name = ''
        # 使用基本面数据中的名称
        stock_name = fundamental.get('company_name', code)

        return jsonify({
            'success': True, 'code': code, 'name': stock_name, 'market': market,
            'indicators': indicators,
            'patterns': patterns,
            'sentiment': {'score': sent_score, 'summary': sent_summary, 'indicators': sent_ind},
            'recommendation': rec,
            'prediction': pred,
            'next_day': next_day,
            't0_trading': t0,
            'fundamental': fundamental,
            'situation': situation,
            'board_info': board_info,
        })
    except Exception as e:
        log(f"单股分析异常: {traceback.format_exc()}")
        return jsonify({'success': False, 'message': f'分析出错: {str(e)}'}), 200

# ========== HTML ==========

HTML_PAGE = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>股票技术形态自动扫描系统</title>
<style>
:root{--bg:#0d1117;--bg2:#161b22;--bg3:#1c2333;--bd:#30363d;--tx:#e6edf3;--tx2:#8b949e;--gn:#3fb950;--rd:#f85149;--bl:#58a6ff;--or:#d2991d;--pr:#a371f7;--yl:#e3b341}
*{margin:0;padding:0;box-sizing:border-box}
body{background:var(--bg);color:var(--tx);font-family:-apple-system,BlinkMacSystemFont,'PingFang SC','Microsoft YaHei',sans-serif;min-height:100vh}
.hd{background:var(--bg2);border-bottom:1px solid var(--bd);padding:12px 24px;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:100}
.hd h1{font-size:1.2rem;font-weight:600;background:linear-gradient(135deg,var(--bl),var(--pr));-webkit-background-clip:text;-webkit-text-fill-color:transparent}
.st{display:flex;align-items:center;gap:12px;font-size:.85rem}
.sd{width:8px;height:8px;border-radius:50%;display:inline-block}
.sd.ok{background:var(--gn);box-shadow:0 0 6px var(--gn)}.sd.err{background:var(--rd)}.sd.busy{background:var(--or);animation:pulse 1s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}
.mc{max-width:1800px;margin:0 auto;padding:16px 24px}
.cp{background:var(--bg3);border:1px solid var(--bd);border-radius:12px;padding:20px;margin-bottom:20px}
.cr{display:flex;flex-wrap:wrap;gap:16px;align-items:center}
.cg{display:flex;flex-direction:column;gap:4px}
.cg label{font-size:.75rem;color:var(--tx2);text-transform:uppercase}
.cg select{background:var(--bg);border:1px solid var(--bd);color:var(--tx);padding:8px 12px;border-radius:8px;font-size:.9rem;min-width:140px}
.pt{display:flex;flex-wrap:wrap;gap:8px;margin-top:8px}
.ptg{padding:6px 14px;border-radius:20px;font-size:.82rem;cursor:pointer;border:1px solid var(--bd);background:var(--bg);color:var(--tx2);transition:all .2s;user-select:none}
.ptg:hover{border-color:var(--bl);color:var(--tx)}
.ptg.on{border-color:var(--gn);background:rgba(63,185,80,.15);color:var(--gn)}
.ptg.sell.on{border-color:var(--rd);color:var(--rd);background:rgba(248,81,73,.15)}
.btn{background:linear-gradient(135deg,var(--bl),var(--pr));color:#fff;border:none;padding:10px 28px;border-radius:8px;font-size:.95rem;font-weight:600;cursor:pointer}
.btn:hover{opacity:.9}.btn:disabled{opacity:.5;cursor:not-allowed}.btn.busy{background:var(--or)}
.btns{padding:6px 14px;font-size:.8rem;border-radius:6px;border:1px solid var(--bd);background:var(--bg);color:var(--tx);cursor:pointer}.btns:hover{border-color:var(--bl)}
.sb{display:flex;gap:24px;padding:12px 0;font-size:.85rem;color:var(--tx2);flex-wrap:wrap;align-items:center}
.sv{color:var(--tx);font-weight:600}
.pbar{flex:1;min-width:200px;height:6px;background:var(--bd);border-radius:3px;overflow:hidden}
.pbar-fill{height:100%;background:linear-gradient(90deg,var(--bl),var(--pr));transition:width .5s;border-radius:3px}
.rw{background:var(--bg3);border:1px solid var(--bd);border-radius:12px;overflow:hidden;margin-bottom:20px}
.thd{padding:12px 20px;border-bottom:1px solid var(--bd);display:flex;justify-content:space-between;align-items:center;font-size:.9rem;font-weight:600}
.ts{overflow:auto;max-height:60vh}
table{width:100%;border-collapse:collapse;font-size:.82rem}
th{background:var(--bg2);color:var(--tx2);font-weight:500;padding:10px 12px;text-align:left;border-bottom:2px solid var(--bd);position:sticky;top:0;z-index:10}
td{padding:8px 12px;border-bottom:1px solid var(--bd);vertical-align:top}
tr:hover td{background:rgba(88,166,255,.04)}.up{color:var(--rd)}.dn{color:var(--gn)}
.pbd{display:flex;flex-wrap:wrap;gap:4px;margin-bottom:4px}
.pb{padding:2px 8px;border-radius:10px;font-size:.72rem;font-weight:500}
.pb.buy{background:rgba(63,185,80,.15);color:var(--gn);border:1px solid rgba(63,185,80,.3)}
.pb.sell{background:rgba(248,81,73,.15);color:var(--rd);border:1px solid rgba(248,81,73,.3)}
.analysis{font-size:.78rem;color:var(--tx2);line-height:1.5;margin-top:4px;padding:6px 10px;background:rgba(88,166,255,.05);border-radius:6px;border-left:3px solid var(--bl)}
.analysis.sell{border-left-color:var(--rd);background:rgba(248,81,73,.05)}
.sent{display:inline-flex;align-items:center;gap:4px;padding:2px 8px;border-radius:10px;font-size:.72rem;font-weight:600}
.sent.hot{background:rgba(248,81,73,.15);color:var(--rd)}
.sent.warm{background:rgba(227,179,65,.15);color:var(--yl)}
.sent.neutral{background:rgba(139,148,158,.15);color:var(--tx2)}
.sent.cool{background:rgba(63,185,80,.15);color:var(--gn)}
.emp{text-align:center;padding:60px 20px;color:var(--tx2)}
.fp{background:rgba(248,81,73,.08);border:1px solid rgba(248,81,73,.3);border-radius:8px;padding:12px 16px;margin-bottom:12px;font-size:.85rem}
.diag{background:var(--bg3);border:1px solid var(--bd);border-radius:8px;padding:16px;margin-bottom:20px}
.diag h3{margin-bottom:12px;font-size:.95rem}
.logp{background:#000;color:#0f0;font-family:monospace;font-size:.7rem;padding:10px;border-radius:8px;max-height:200px;overflow:auto;white-space:pre-wrap;display:none}
.sort-btns{display:flex;gap:6px;align-items:center}
.sort-btn{padding:3px 10px;border-radius:12px;font-size:.72rem;cursor:pointer;border:1px solid var(--bd);background:var(--bg);color:var(--tx2);transition:all .2s;white-space:nowrap}
.sort-btn:hover{border-color:var(--bl);color:var(--tx)}
.sort-btn.active{border-color:var(--bl);background:rgba(88,166,255,.15);color:var(--bl)}
::-webkit-scrollbar{width:6px;height:6px}::-webkit-scrollbar-thumb{background:var(--bd);border-radius:3px}
@media(max-width:768px){.hd{padding:10px 16px}.hd h1{font-size:1rem}.mc{padding:12px}.cr{flex-direction:column;align-items:stretch}}
</style>
</head>
<body>
<div class="hd"><div><h1>📊 股票技术形态自动扫描系统</h1><div style="font-size:.75rem;color:var(--tx2);margin-top:2px">9种形态+情绪因子 | 换手率/量比/连阳/振幅/涨速 | A股+港股通全量扫描 | <span id="auctionStatus" style="color:var(--or)"></span></div></div>
<div class="st"><span><span class="sd ok" id="sd"></span> <span id="stx">就绪</span></span><span style="color:var(--tx2)" id="clk"></span></div></div>
<div class="mc">
<div class="cp"><div class="cr">
<div class="cg"><label>市场</label><div style="display:flex;gap:8px"><span class="ptg on" id="mA" onclick="tM('A')">A股</span><span class="ptg on" id="mHK" onclick="tM('HK')">港股通</span></div></div>
<div class="cg"><label>自动刷新</label><select id="ar"><option value="0" selected>手动</option><option value="600">10分钟</option><option value="1800">30分钟</option></select></div>
<div style="display:flex;align-items:flex-end;gap:8px"><input id="stockCode" placeholder="输入股票代码如000001" style="background:var(--bg);border:1px solid var(--bd);color:var(--tx);padding:10px 14px;border-radius:8px;font-size:.9rem;width:170px"><select id="stockMkt" style="background:var(--bg);border:1px solid var(--bd);color:var(--tx);padding:10px 8px;border-radius:8px;font-size:.9rem"><option value="A">A股</option><option value="HK">港股</option></select><button class="btn" onclick="analyzeOne()" style="background:linear-gradient(135deg,var(--pr),#7c3aed)">🔎 分析</button><button class="btn" id="sb" onclick="scan(200)" style="background:linear-gradient(135deg,var(--gn),#2da44e)">测试200只</button><button class="btn" onclick="scan(null)">全量扫描</button><button class="btns" onclick="testConn()">诊断</button></div>
</div>
<div class="pt"><span style="font-size:.75rem;color:var(--tx2);margin-right:8px;line-height:32px">形态:</span>
<span class="ptg on" data-p="bottom_divergence" onclick="tP(this)">底背离</span>
<span class="ptg on" data-p="uptrend" onclick="tP(this)">上升趋势(左侧)</span>
<span class="ptg on" data-p="first_limit_up" onclick="tP(this)">首板</span>
<span class="ptg on" data-p="consecutive_limit_up" onclick="tP(this)">连板</span>
<span class="ptg on" data-p="potential_first_board" onclick="tP(this)">次日可能首板</span>
<span class="ptg on" data-p="potential_continue_board" onclick="tP(this)">次日可能再板</span>
<span class="ptg sell on" data-p="top_divergence" onclick="tP(this)">顶背离</span>
<span class="ptg on" data-p="bottom_launch" onclick="tP(this)">底部启动</span>
</div></div>
<div id="errs"></div>
<div class="sb"><div>进度: <span class="sv" id="ss">-</span></div><div>匹配: <span class="sv" id="sm">-</span></div><div>耗时: <span class="sv" id="stm">-</span></div><div class="pbar"><div class="pbar-fill" id="pfill" style="width:0%"></div></div></div>
<div class="rw"><div class="thd"><span>扫描结果（每只匹配股票附分析说明）</span><div class="sort-btns"><span class="sort-btn active" data-sort="strength" onclick="sortBy(this)">按强度</span><span class="sort-btn" data-sort="sentiment" onclick="sortBy(this)">🔥按情绪</span><span class="sort-btn" data-sort="patterns" onclick="sortBy(this)">按形态数</span><span id="rc" style="color:var(--tx2);margin-left:8px">等待扫描...</span></div></div>
<div class="ts"><table><thead><tr><th style="width:50px">市场</th><th style="width:70px">代码</th><th style="width:75px">名称</th><th style="width:55px">现价</th><th style="width:55px">涨跌</th><th style="width:55px">情绪</th><th style="width:170px">形态</th><th>分析说明(含情绪)</th></tr></thead>
<tbody id="rb"><tr><td colspan="8"><div class="emp"><p style="font-size:2.5rem">📊</p><p>点击 <b>"测试200只"</b> 快速测试 | <b>"全量扫描"</b> 扫描全部</p><p style="font-size:.8rem;color:var(--tx2)">含情绪因子: 换手率/量比/连阳/振幅/涨速</p></div></td></tr></tbody></table></div></div>
<div id="logPanel" class="logp"></div>

<!-- 持仓分析 -->
<div class="rw" style="margin-top:20px">
<div class="thd" style="cursor:pointer" onclick="let p=document.getElementById('posPanel');p.style.display=p.style.display==='none'?'block':'none'"><span>📋 持仓量化分析 (输入持仓信息获取操作方案)</span><span style="font-size:.8rem;color:var(--tx2)">点击展开/收起</span></div>
<div id="posPanel" style="padding:20px;display:none">
<div style="display:flex;flex-wrap:wrap;gap:12px;align-items:flex-end;margin-bottom:16px">
<div class="cg"><label>股票代码</label><input id="posCode" placeholder="如000001" style="background:var(--bg);border:1px solid var(--bd);color:var(--tx);padding:10px 14px;border-radius:8px;font-size:.9rem;width:120px"></div>
<div class="cg"><label>市场</label><select id="posMkt" style="background:var(--bg);border:1px solid var(--bd);color:var(--tx);padding:10px;border-radius:8px;font-size:.9rem"><option value="A">A股</option><option value="HK">港股</option></select></div>
<div class="cg"><label>买入价格(元)</label><input id="posPrice" type="number" step="0.01" placeholder="如12.50" style="background:var(--bg);border:1px solid var(--bd);color:var(--tx);padding:10px 14px;border-radius:8px;font-size:.9rem;width:120px"></div>
<div class="cg"><label>持有股数</label><input id="posShares" type="number" placeholder="如1000" style="background:var(--bg);border:1px solid var(--bd);color:var(--tx);padding:10px 14px;border-radius:8px;font-size:.9rem;width:120px"></div>
<div class="cg"><label>目标盈利(元,可选)</label><input id="posTarget" type="number" placeholder="如5000" style="background:var(--bg);border:1px solid var(--bd);color:var(--tx);padding:10px 14px;border-radius:8px;font-size:.9rem;width:120px"></div>
<button class="btn" onclick="analyzePos()" style="background:linear-gradient(135deg,var(--pr),#7c3aed)">🔍 分析持仓</button>
</div>
<div id="posResult"></div>
</div></div>

<!-- 单股分析弹窗 -->
<div id="analyzeModal" style="display:none;position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,.75);z-index:1000;justify-content:center;align-items:flex-start;padding-top:30px;overflow-y:auto">
<div style="background:var(--bg3);border:1px solid var(--bd);border-radius:16px;padding:28px;max-width:900px;width:95%;margin-bottom:40px">
<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:20px;border-bottom:1px solid var(--bd);padding-bottom:14px">
<h2 style="margin:0;font-size:1.1rem" id="amTitle">股票深度分析</h2>
<button onclick="document.getElementById('analyzeModal').style.display='none'" style="background:none;border:none;color:var(--tx);font-size:1.5rem;cursor:pointer">&times;</button>
</div>
<div id="amBody">加载中...</div>
</div></div>
</div>
<script>
let mk=['A','HK'];let pt=['bottom_divergence','uptrend','first_limit_up','consecutive_limit_up','potential_first_board','potential_continue_board','top_divergence','bottom_launch'];
let timer=null;let busy=false;let polling=null;
function checkAuction(){let n=new Date(),h=n.getHours(),m=n.getMinutes(),d=n.getDay(),el=document.getElementById('auctionStatus');if(d===0||d===6){el.textContent='周末休市';el.style.color='var(--tx2)';return}if(h===9&&m>=15&&m<=25){el.textContent='⚡竞价进行中 9:'+String(m).padStart(2,'0');el.style.color='var(--or)'}else if(h===9&&m>=25&&m<=30){el.textContent='竞价结束 等待开盘';el.style.color='var(--yl)'}else if((h===9&&m>=30)||(h>=10&&h<11)||(h===11&&m<=30)||(h===13&&m>=0)||(h>=14&&h<15)){el.textContent='盘中交易';el.style.color='var(--gn)'}else if((h>=0&&h<9)||(h===9&&m<15)){el.textContent='盘前';el.style.color='var(--tx2)'}else{el.textContent='已收盘';el.style.color='var(--tx2)'}}
setInterval(()=>{document.getElementById('clk').textContent=new Date().toLocaleString('zh-CN',{hour12:false});checkAuction()},1000);
function ss(s,t){document.getElementById('sd').className='sd '+s;document.getElementById('stx').textContent=t}
function tM(m){let e=document.getElementById('m'+m),i=mk.indexOf(m);i>=0?(mk.splice(i,1),e.classList.remove('on')):(mk.push(m),e.classList.add('on'))}
function tP(e){e.classList.toggle('on');let p=e.dataset.p,i=pt.indexOf(p);i>=0?pt.splice(i,1):pt.push(p)}

async function testConn(){
  ss('busy','诊断中...');document.getElementById('errs').innerHTML='';document.getElementById('logPanel').style.display='block';document.getElementById('logPanel').textContent='诊断中...';
  try{
    let r=await fetch('/api/test'),d=await r.json();
    let h='<div class="diag"><h3>🔍 诊断结果</h3>';

    // 数据源状态单独显示
    let ds=d.results['数据源状态'];
    if(ds){
      h+=`<div style="background:var(--bg);border:1px solid var(--bl);border-radius:8px;padding:12px;margin-bottom:12px">
        <div style="font-weight:600;color:var(--bl);margin-bottom:8px">📊 数据源状态</div>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:6px;font-size:.82rem">
          <div>Tushare Pro: ${ds.tushare}</div>
          <div>Baostock: ${ds.baostock}</div>
          <div>东方财富: ${ds['东方财富']}</div>
          <div style="font-weight:600;color:var(--gn)">当前A股源: ${ds['当前A股数据源']}</div>
        </div>
      </div>`;
      delete d.results['数据源状态'];
    }

    for(let[k,v]of Object.entries(d.results)){
      if(typeof v==='object')continue; // 跳过对象类型的值
      let ok=typeof v==='string' && v.startsWith('OK');
      h+=`<div style="padding:6px 0;border-bottom:1px solid var(--bd)"><span style="color:${ok?'var(--gn)':'var(--rd)'}">${ok?'✅':'❌'}</span> <b>${k}:</b> ${v}</div>`;
    }
    h+='</div>';if(d.logs)document.getElementById('logPanel').textContent=d.logs.join('\n');
    document.getElementById('errs').innerHTML=h;ss('ok','诊断完成');
  }catch(e){document.getElementById('errs').innerHTML=`<div class="fp">诊断失败: ${e.message}</div>`;ss('err','失败')}
}

function startPolling(){stopPolling();polling=setInterval(async()=>{try{let r=await fetch('/api/scan_status'),d=await r.json();if(d.running){document.getElementById('ss').textContent=`${d.progress}/${d.total}`;document.getElementById('sm').textContent=d.matched;let pct=d.total>0?Math.round(d.progress/d.total*100):0;document.getElementById('pfill').style.width=pct+'%'}}catch(e){}},1000)}
function stopPolling(){if(polling){clearInterval(polling);polling=null}}

let allScanResults=[]; let scanOffset=0; let scanAborted=false;

async function scan(limit){
  if(busy)return;if(mk.length===0){alert('请选择市场');return}if(pt.length===0){alert('请选择形态');return}
  busy=true;scanAborted=false;allScanResults=[];scanOffset=0;
  document.getElementById('sb').disabled=true;  // 只禁用扫描按钮，不影响搜索
  document.getElementById('errs').innerHTML='';document.getElementById('pfill').style.width='0%';
  document.getElementById('ss').textContent='0/0';document.getElementById('sm').textContent='0';
  ss('busy','扫描中...');
  document.getElementById('logPanel').style.display='block';
  let batchSize=limit||30; let totalTime=0;

  async function scanBatch(){
    if(scanAborted){finishScan();return}
    try{
      let body={markets:mk,patterns:pt,batch_size:batchSize,offset:scanOffset};
      let r=await fetch('/api/scan',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
      let d=await r.json();
      if(!d.success){ss('err','失败');busy=false;return}
      totalTime+=d.elapsed||0;
      allScanResults=allScanResults.concat(d.results||[]);
      // 去重排序
      let seen=new Set();allScanResults=allScanResults.filter(r=>{let k=r.code+r.market;if(seen.has(k))return false;seen.add(k);return true});
      allScanResults.sort((a,b)=>b.patterns.length-a.patterns.length||(b.patterns[0]?.strength||0)-(a.patterns[0]?.strength||0));
      scanOffset=d.offset+batchSize;
      let total=d.total_available||0;
      let pct=total>0?Math.round(Math.min(scanOffset,total)/total*100):0;
      document.getElementById('ss').textContent=`${Math.min(scanOffset,total)}/${total}`;
      document.getElementById('sm').textContent=allScanResults.length;
      document.getElementById('stm').textContent=totalTime.toFixed(0)+'s';
      document.getElementById('pfill').style.width=pct+'%';
      if(d.logs)document.getElementById('logPanel').textContent=d.logs.join('\n');
      document.getElementById('rc').textContent='已匹配 '+allScanResults.length+' 只';
      render(allScanResults);
      if(d.has_more&&!scanAborted){
        setTimeout(scanBatch,500);  // 继续下一批
      }else{
        finishScan();
      }
    }catch(e){ss('err','失败');busy=false}
  }

  function finishScan(){
    scanAborted=true;
    document.getElementById('rc').textContent='共 '+allScanResults.length+' 只匹配';
    document.getElementById('pfill').style.width='100%';
    ss('ok','扫描完成');
    busy=false;document.getElementById('sb').disabled=false;
  }

  await scanBatch();
}

function stopAR(){scanAborted=true;if(timer){clearInterval(timer);timer=null}document.getElementById('stb').style.display='none'}

let currentSort='strength';
function sortBy(el){
  document.querySelectorAll('.sort-btn').forEach(b=>b.classList.remove('active'));
  el.classList.add('active');
  currentSort=el.dataset.sort;
  if(!allScanResults.length)return;
  if(currentSort==='sentiment'){
    allScanResults.sort((a,b)=>((b.sentiment||{}).score||0)-((a.sentiment||{}).score||0));
  }else if(currentSort==='patterns'){
    allScanResults.sort((a,b)=>b.patterns.length-a.patterns.length);
  }else{
    allScanResults.sort((a,b)=>b.patterns.length-a.patterns.length||(b.patterns[0]?.strength||0)-(a.patterns[0]?.strength||0));
  }
  render(allScanResults);
}
function render(rs){
  let tb=document.getElementById('rb');
  if(!rs.length){tb.innerHTML='<tr><td colspan="8"><div class="emp"><p>未找到匹配形态</p><p style="font-size:.8rem;color:var(--tx2)">当前市场暂无满足条件的股票</p></div></td></tr>';return}
  tb.innerHTML=rs.map((s,i)=>{
    let cc=s.change_pct>=0?'up':'dn',cs=s.change_pct>=0?'+':'';
    // 情绪徽章
    let se=s.sentiment||{};
    let sCls=se.score>=65?'hot':se.score>=50?'warm':se.score>=35?'neutral':'cool';
    let sEmoji=se.score>=65?'🔥':se.score>=50?'😊':se.score>=35?'😐':'😟';
    let sentHtml=`<span class="sent ${sCls}" title="${se.summary||''}">${sEmoji}${se.score||'-'}</span>`;
    let bs=s.patterns.map(p=>`<span class="pb ${p.signal}">${p.name}(${p.strength})</span>`).join('');
    let analyses=s.patterns.map(p=>`<div class="analysis ${p.signal}">${p.analysis||''}</div>`).join('');
    return `<tr><td>${s.market==='A'?'A股':'港股'}</td><td style="font-family:monospace">${s.code}</td><td>${s.name}</td><td>${s.price.toFixed(2)}</td><td class="${cc}">${cs}${s.change_pct.toFixed(2)}%</td><td>${sentHtml}</td><td><div class="pbd">${bs}</div></td><td>${analyses}</td></tr>`;
  }).join('');
}
document.getElementById('ar').addEventListener('change',function(){if(this.value==='0'&&timer){clearInterval(timer);timer=null}});

// ======= 单股分析 =======
async function analyzeOne(){
  let code=document.getElementById('stockCode').value.trim();
  if(!code){alert('请输入股票代码');return}
  let mkt=document.getElementById('stockMkt').value;
  let modal=document.getElementById('analyzeModal');
  let body=document.getElementById('amBody');
  body.innerHTML='<div style="text-align:center;padding:40px"><div style="width:24px;height:24px;border:3px solid var(--bd);border-top-color:var(--bl);border-radius:50%;animation:spin .6s linear infinite;display:inline-block"></div><p style="margin-top:12px;color:var(--tx2)">正在深度分析...</p></div>';
  modal.style.display='flex';
  document.getElementById('amTitle').textContent=`📊 ${code} 深度量化分析`;
  try{
    let r=await fetch(`/api/analyze/${code}?market=${mkt}`),d=await r.json();
    if(!d.success){body.innerHTML=`<div class="fp"><span class="et">分析失败:</span> ${d.message}</div>`;return}
    let ind=d.indicators,pat=d.patterns||[],sent=d.sentiment||{},rec=d.recommendation||{};
    let pChg=ind.change_pct||0,chgCls=pChg>=0?'color:var(--rd)':'color:var(--gn)',chgSgn=pChg>=0?'+':'';
    let sCls=sent.score>=65?'hot':sent.score>=50?'warm':sent.score>=35?'neutral':'cool';
    let sEmoji=sent.score>=65?'🔥':sent.score>=50?'😊':sent.score>=35?'😐':'😟';
    // 构建HTML
    let h=`<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:16px">`;
    // 基本面卡片
    h+=`<div style="background:var(--bg);border-radius:10px;padding:16px"><h4 style="margin:0 0 10px;font-size:.85rem;color:var(--bl)">📋 基本信息</h4>
      <div style="font-size:1.4rem;font-weight:700">${d.name||d.code} <span style="font-size:.85rem;color:var(--tx2)">${d.code}</span></div>
      <div style="font-size:1.3rem;margin:6px 0"><span style="${chgCls}">${ind.price} <small>${chgSgn}${pChg}%</small></span></div>
      <div style="color:var(--tx2);font-size:.8rem">市场: ${mkt==='A'?'A股':'港股'}</div></div>`;
    // 基本面卡片(全宽、详细)
    let fund=d.fundamental||{},sit=d.situation||{};
    h+=`<div style="background:var(--bg);border-radius:10px;padding:16px;grid-column:1/-1"><h4 style="margin:0 0 12px;font-size:.85rem;color:var(--yl)">🏢 公司基本面</h4>`;
    // 公司介绍（F10数据）
    if(fund.company_intro)h+=`<div style="font-size:.83rem;line-height:1.7;margin-bottom:12px;padding:12px;background:var(--bg3);border-radius:8px;border-left:3px solid var(--bl)">📖 ${fund.company_intro}</div>`;
    // 主营构成
    if(fund.business_detail)h+=`<div style="font-size:.82rem;line-height:1.6;margin-bottom:10px;padding:10px;background:rgba(63,185,80,.05);border-radius:8px;border-left:3px solid var(--gn)">💼 <b>主营构成:</b> ${fund.business_detail}</div>`;
    // 主营业务
    if(fund.main_business&&!fund.business_detail)h+=`<div style="font-size:.82rem;color:var(--tx);margin-bottom:8px">💼 <b>主营业务:</b> ${fund.main_business}</div>`;
    // 研报级概述（多段落格式）
    let summary=fund.summary||'';
    if(summary){
      let paras=summary.split('\n').filter(p=>p.trim());
      h+=`<div style="font-size:.84rem;line-height:1.7;margin-bottom:10px">`;
      paras.forEach(p=>{
        let isHeader=p.startsWith('【');
        let headerEnd=p.indexOf('】');
        if(isHeader&&headerEnd>1){
          let header=p.substring(1,headerEnd);
          let content=p.substring(headerEnd+1);
          h+=`<div style="margin-bottom:6px"><b style="color:var(--yl)">${header}：</b>${content}</div>`;
        }else{
          h+=`<div style="margin-bottom:4px">${p}</div>`;
        }
      });
      h+=`</div>`;
    }
    // 5年财务趋势表
    let trend=fund.annual_trend||{};
    let years=Object.keys(trend).sort();
    if(years.length>=2){
      h+=`<div style="background:var(--bg3);border-radius:8px;padding:12px;margin-bottom:10px"><div style="font-size:.78rem;color:var(--yl);font-weight:600;margin-bottom:6px">📊 近5年财务趋势</div>`;
      h+=`<div style="overflow-x:auto"><table style="width:100%;font-size:.75rem;border-collapse:collapse"><thead><tr style="background:var(--bg)"><th style="padding:4px 10px;border:1px solid var(--bd)">年份</th><th style="padding:4px 10px;border:1px solid var(--bd)">营收(亿)</th><th style="padding:4px 10px;border:1px solid var(--bd)">净利(亿)</th><th style="padding:4px 10px;border:1px solid var(--bd)">ROE%</th><th style="padding:4px 10px;border:1px solid var(--bd)">EPS</th><th style="padding:4px 10px;border:1px solid var(--bd)">毛利率%</th></tr></thead><tbody>`;
      years.forEach(yr=>{
        let d=trend[yr];
        let prev=trend[yr-1]||{};
        let color=(v,pv)=>v>(pv||0)?'color:var(--rd)':v<(pv||0)?'color:var(--gn)':'';
        h+=`<tr><td style="padding:4px 10px;border:1px solid var(--bd);font-weight:600">${yr}</td>
          <td style="padding:4px 10px;border:1px solid var(--bd);${color(d.revenue,prev.revenue)}">${d.revenue?d.revenue.toFixed(1):'-'}</td>
          <td style="padding:4px 10px;border:1px solid var(--bd);${color(d.profit,prev.profit)}">${d.profit?d.profit.toFixed(1):'-'}</td>
          <td style="padding:4px 10px;border:1px solid var(--bd)">${d.roe?d.roe.toFixed(1):'-'}</td>
          <td style="padding:4px 10px;border:1px solid var(--bd)">${d.eps?d.eps.toFixed(3):'-'}</td>
          <td style="padding:4px 10px;border:1px solid var(--bd)">${d.gross_margin?d.gross_margin.toFixed(1):'-'}</td></tr>`;
      });
      h+=`</tbody></table></div></div>`;
    }
    // 公司详细信息
    let extInfo=[];
    if(fund.full_name)extInfo.push(['公司全称',fund.full_name]);
    if(fund.legal_person)extInfo.push(['法人代表',fund.legal_person]);
    if(fund.establish_date)extInfo.push(['成立日期',fund.establish_date]);
    if(fund.ipo_date)extInfo.push(['上市日期',fund.ipo_date]);
    if(fund.total_shares)extInfo.push(['总股本',fund.total_shares]);
    if(fund.circulation_shares)extInfo.push(['流通股本',fund.circulation_shares]);
    if(fund.employees)extInfo.push(['员工人数',fund.employees+'人']);
    if(fund.reg_addr)extInfo.push(['注册地址',fund.reg_addr]);
    if(fund.website)extInfo.push(['公司网址',fund.website]);
    if(extInfo.length>0){
      h+=`<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:4px;margin-bottom:10px">`;
      extInfo.forEach(([l,v])=>h+=`<div style="font-size:.75rem;padding:3px 0"><span style="color:var(--tx2)">${l}:</span> ${v}</div>`);
      h+=`</div>`;
    }
    // 风险提示
    if(fund.risk_note)h+=`<div style="font-size:.76rem;color:var(--or);background:rgba(227,179,65,.06);border-radius:6px;padding:8px 10px;margin-bottom:8px">⚠️ ${fund.risk_note}</div>`;
    // 财务指标网格
    let finItems=[];
    if(fund.revenue)finItems.push(['营业收入',fund.revenue, fund.revenue_growth||'']);
    if(fund.net_profit)finItems.push(['归母净利润',fund.net_profit, fund.profit_growth||'']);
    if(fund.eps)finItems.push(['每股收益(EPS)',fund.eps,'']);
    if(fund.roe)finItems.push(['净资产收益率(ROE)',fund.roe,'']);
    if(fund.gross_margin)finItems.push(['毛利率',fund.gross_margin,'']);
    if(fund.net_margin)finItems.push(['净利率',fund.net_margin,'']);
    if(fund.debt_ratio)finItems.push(['资产负债率',fund.debt_ratio,'']);
    if(fund.total_assets)finItems.push(['总资产',fund.total_assets,'']);
    if(fund.holders)finItems.push(['股东人数',fund.holders,'']);
    if(finItems.length>0){
      h+=`<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:6px;margin-bottom:10px">`;
      finItems.forEach(([l,v,g])=>h+=`<div style="padding:6px 10px;background:var(--bg3);border-radius:6px;font-size:.78rem"><span style="color:var(--tx2)">${l}</span> <span style="font-weight:600">${v}</span>${g?` <span style="font-size:.7rem;color:${g.startsWith('-')?'var(--gn)':'var(--rd)'}">${g}</span>`:''}</div>`);
      h+=`</div>`;
    }
    h+=`</div>`;
    // 板块归属+同类股票
    let board=d.board_info||{};
    if(board.indexes&&board.indexes.length||board.peers&&board.peers.length){
      h+=`<div style="background:var(--bg);border-radius:10px;padding:16px;grid-column:1/-1"><h4 style="margin:0 0 10px;font-size:.85rem;color:var(--pr)">📌 板块归属与同类股票</h4>`;
      if(board.indexes&&board.indexes.length)h+=`<div style="margin-bottom:8px">📊 所属指数: ${board.indexes.map(i=>`<span style="padding:2px 8px;background:rgba(88,166,255,.1);border-radius:6px;font-size:.75rem;margin:0 3px">${i}</span>`).join('')}</div>`;
      if(board.industry_note)h+=`<div style="font-size:.8rem;color:var(--tx2);margin-bottom:8px">${board.industry_note}</div>`;
      if(board.peers&&board.peers.length){
        // 把当前股票也加入对比表
        // 当前股票也加入对比(用基本面数据的财务指标)
        let selfData={code:d.code,name:d.name||code,price:d.indicators.price,chg_5d:d.indicators.change_pct,chg_20d:sit.chg_20d||0,chg_60d:sit.chg_60d||0,trend:'当前',vol_level:'-',is_self:true,revenue:fund.revenue,profit:fund.net_profit};
        let allPeers=[selfData,...board.peers];
        h+=`<div style="overflow-x:auto;margin-top:8px"><table style="width:100%;font-size:.75rem;border-collapse:collapse"><thead><tr style="background:var(--bg)"><th>代码</th><th>名称</th><th>现价</th><th>近5日</th><th>近20日</th><th>近60日</th><th>趋势</th><th>量能</th><th>营收</th><th>净利</th></tr></thead><tbody>`;
        allPeers.forEach((p,i)=>{
          let rowStyle=p.is_self?'background:rgba(88,166,255,.08);font-weight:600':'';
          let c=(v)=>v>=0?'color:var(--rd)':'color:var(--gn)';
          let sgn=(v)=>v>0?'+':'';
          let tC=p.trend==='上升'?'color:var(--rd)':p.trend==='下降'?'color:var(--gn)':p.trend==='当前'?'color:var(--bl)':'';
          h+=`<tr style="${rowStyle}"><td style="padding:5px 8px;border:1px solid var(--bd)">${p.code}</td>
            <td style="padding:5px 8px;border:1px solid var(--bd)">${p.name}${p.is_self?' (你查询的)':''}</td>
            <td style="padding:5px 8px;border:1px solid var(--bd)">${p.price||'-'}</td>
            <td style="padding:5px 8px;border:1px solid var(--bd);${c(p.chg_5d||0)}">${p.chg_5d!=null?sgn(p.chg_5d)+p.chg_5d.toFixed(1)+'%':'-'}</td>
            <td style="padding:5px 8px;border:1px solid var(--bd);${c(p.chg_20d||0)}">${p.chg_20d!=null?sgn(p.chg_20d)+p.chg_20d.toFixed(1)+'%':'-'}</td>
            <td style="padding:5px 8px;border:1px solid var(--bd);${c(p.chg_60d||0)}">${p.chg_60d!=null?sgn(p.chg_60d)+p.chg_60d.toFixed(1)+'%':'-'}</td>
            <td style="padding:5px 8px;border:1px solid var(--bd);${tC}">${p.trend||'-'}</td>
            <td style="padding:5px 8px;border:1px solid var(--bd)">${p.vol_level||'-'}</td>
            <td style="padding:5px 8px;border:1px solid var(--bd)">${p.revenue||'-'}</td>
            <td style="padding:5px 8px;border:1px solid var(--bd)">${p.profit||'-'}</td></tr>`;
        });
        h+=`</tbody></table></div>`;
      }
      h+=`</div>`;
    }
    // 时局分析卡片
    if(sit.summary)h+=`<div style="background:var(--bg);border-radius:10px;padding:16px;grid-column:1/-1"><h4 style="margin:0 0 8px;font-size:.85rem;color:var(--bl)">📊 时局与技术分析</h4>
      <div style="font-size:.84rem;line-height:1.6">${sit.summary}</div>
      ${sit.tech_phase?`<div style="margin-top:6px;font-size:.78rem;color:var(--tx2)">🔍 技术阶段: ${sit.tech_phase}</div>`:''}
    </div>`;
    // 建议卡片
    h+=`<div style="background:var(--bg);border-radius:10px;padding:16px"><h4 style="margin:0 0 10px;font-size:.85rem;color:var(--or)">🎯 投资建议</h4>
      <div style="font-size:1.6rem;font-weight:700">${rec.icon||'📊'} ${rec.score||'-'}分</div>
      <div style="font-weight:600;margin:4px 0">${rec.level||'-'}</div>
      <div style="font-size:.8rem;color:var(--tx2);line-height:1.4">${rec.action||''}</div></div>`;
    // 情绪卡片
    h+=`<div style="background:var(--bg);border-radius:10px;padding:16px"><h4 style="margin:0 0 10px;font-size:.85rem;color:var(--pr)">💬 市场情绪</h4>
      <div style="font-size:1.4rem;font-weight:700"><span class="sent ${sCls}">${sEmoji} ${sent.score||'-'}</span></div>
      <div style="font-size:.8rem;color:var(--tx2);margin-top:4px">换手率: ${sent.indicators?.turnover||'-'}% | 量比5日: ${sent.indicators?.vol_ratio_5d||'-'}x</div>
      <div style="font-size:.8rem;color:var(--tx2)">连阳: ${sent.indicators?.consec_up>0?sent.indicators.consec_up+'日':(sent.indicators?.consec_up<0?'连阴'+Math.abs(sent.indicators.consec_up)+'日':'无')}</div></div>`;
    h+=`</div>`;
    // 技术指标表格
    h+=`<div style="background:var(--bg);border-radius:10px;padding:16px;margin-top:16px"><h4 style="margin:0 0 10px;font-size:.85rem;color:var(--gn)">📐 技术指标</h4>
      <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(140px,1fr));gap:6px;font-size:.8rem">`;
    let its=[['MACD DIF',ind.macd_dif,''],['MACD DEA',ind.macd_dea,''],['MACD柱',ind.macd_bar,(ind.macd_bar||0)>0?'color:var(--rd)':'color:var(--gn)'],['RSI(14)',ind.rsi,''],['KDJ-K',ind.kdj_k,''],['KDJ-D',ind.kdj_d,''],['KDJ-J',ind.kdj_j,''],['布林上轨',ind.boll_upper,''],['布林中轨',ind.boll_mid,''],['布林下轨',ind.boll_lower,''],['带宽%',ind.boll_bandwidth,''],['MA5',ind.ma5,''],['MA10',ind.ma10,''],['MA20',ind.ma20,''],['MA60',ind.ma60,'']];
    its.forEach(([l,v,s])=>h+=`<div style="padding:4px 8px;background:var(--bg3);border-radius:6px"><span style="color:var(--tx2)">${l}</span> <span style="font-weight:600;${s}">${v!=null?v:'-'}</span></div>`);
    h+=`</div></div>`;
    // 检测到的形态
    if(pat.length>0){
      h+=`<div style="background:var(--bg);border-radius:10px;padding:16px;margin-top:16px"><h4 style="margin:0 0 10px;font-size:.85rem;color:var(--gn)">✅ 触发形态 (${pat.length}个)</h4>`;
      pat.forEach(p=>h+=`<div class="analysis ${p.signal}" style="margin-bottom:8px">${p.analysis||''}</div>`);
      h+=`</div>`;
    }
    // 多空理由
    if(rec.reasons&&rec.reasons.length>0){
      h+=`<div style="background:var(--bg);border-radius:10px;padding:16px;margin-top:12px"><h4 style="margin:0 0 8px;font-size:.85rem;color:var(--gn)">✅ 看多理由</h4>`;
      rec.reasons.forEach(r=>h+=`<div style="padding:3px 0;font-size:.82rem">• ${r}</div>`);
      h+=`</div>`;
    }
    if(rec.risks&&rec.risks.length>0){
      h+=`<div style="background:var(--bg);border-radius:10px;padding:16px;margin-top:8px"><h4 style="margin:0 0 8px;font-size:.85rem;color:var(--rd)">⚠️ 风险提示</h4>`;
      rec.risks.forEach(r=>h+=`<div style="padding:3px 0;font-size:.82rem;color:var(--rd)">• ${r}</div>`);
      h+=`</div>`;
    }
    // 买卖点预测
    let pred=d.prediction||{};
    if(pred.current_price){
      h+=`<div style="background:var(--bg);border-radius:10px;padding:16px;margin-top:16px"><h4 style="margin:0 0 12px;font-size:.85rem;color:var(--yl)">🎯 买卖点与涨幅预测</h4>`;
      // 买入点
      h+=`<div style="margin-bottom:12px"><span style="font-weight:600;color:var(--gn)">📥 最佳买入区间: ${pred.buy_zone||'-'}</span>
        <div style="font-size:.78rem;color:var(--tx2);margin-top:2px">${pred.buy_explanation||''}</div>`;
      if(pred.supports)pred.supports.forEach(s=>h+=`<span style="display:inline-block;margin:3px 4px;padding:2px 8px;background:rgba(63,185,80,.1);border-radius:8px;font-size:.72rem">${s.label}: ${s.level} [${s.strength}]</span>`);
      h+=`</div>`;
      // 卖出点
      h+=`<div style="margin-bottom:12px"><span style="font-weight:600;color:var(--rd)">📤 最佳卖出区间: ${pred.sell_zone||'-'}</span>
        <div style="font-size:.78rem;color:var(--tx2);margin-top:2px">${pred.sell_explanation||''}</div>`;
      if(pred.resistances)pred.resistances.forEach(s=>h+=`<span style="display:inline-block;margin:3px 4px;padding:2px 8px;background:rgba(248,81,73,.1);border-radius:8px;font-size:.72rem">${s.label}: ${s.level} [${s.strength}]</span>`);
      h+=`</div>`;
      // 目标预测
      h+=`<div style="margin-bottom:12px"><span style="font-weight:600;color:var(--bl)">📈 涨幅目标:</span>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:6px">`;
      if(pred.targets)pred.targets.forEach(t=>h+=`<div style="background:var(--bg3);border-radius:8px;padding:8px 10px">
        <div style="font-weight:600;font-size:.85rem">${t.name}: <span style="color:var(--rd)">${t.price} (+${t.upside}%)</span></div>
        <div style="font-size:.72rem;color:var(--tx2)">⏱ ${t.timeframe||''} | 风报比 ${t.rr_ratio||'-'}:1</div>
        <div style="font-size:.7rem;color:var(--tx2)">方法: ${t.method||''}</div></div>`);
      h+=`</div></div>`;
      // 止损
      h+=`<div style="margin-bottom:6px"><span style="font-weight:600;color:var(--or)">🛑 建议止损: ${pred.stop_loss||'-'} (${pred.stop_loss_pct||'-'}%)</span></div>`;
      // 一句话总结
      h+=`<div style="font-size:.82rem;color:var(--tx);background:rgba(163,113,247,.08);border-radius:8px;padding:10px;line-height:1.5">${pred.summary||''}</div></div>`;
    }
    // 次日走势预测
    let nd=d.next_day||{};
    if(nd.current_price){
      let upCls=nd.up_probability>=55?'color:var(--rd)':nd.up_probability>=45?'color:var(--or)':'color:var(--gn)';
      let dnCls=nd.down_probability>=55?'color:var(--gn)':nd.down_probability>=45?'color:var(--or)':'color:var(--rd)';
      h+=`<div style="background:linear-gradient(135deg,rgba(227,179,65,.06),rgba(88,166,255,.06));border:1px solid rgba(227,179,65,.25);border-radius:10px;padding:16px;margin-top:16px">
        <h4 style="margin:0 0 10px;font-size:.85rem;color:var(--yl)">🔮 次日走势预测</h4>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:10px">
          <div style="text-align:center;background:var(--bg);border-radius:8px;padding:10px">
            <div style="font-size:.75rem;color:var(--tx2)">上涨概率</div>
            <div style="font-size:1.6rem;font-weight:700;${upCls}">${nd.up_probability||'-'}%</div></div>
          <div style="text-align:center;background:var(--bg);border-radius:8px;padding:10px">
            <div style="font-size:.75rem;color:var(--tx2)">下跌概率</div>
            <div style="font-size:1.6rem;font-weight:700;${dnCls}">${nd.down_probability||'-'}%</div></div>
        </div>
        <div style="background:var(--bg);border-radius:8px;padding:10px;margin-bottom:8px">
          <div style="font-weight:600;font-size:.9rem">📐 预计区间: <span style="color:var(--bl)">${nd.expected_range||'-'}</span></div>
          <div style="font-size:.82rem;color:var(--or);margin-top:2px">概率最大区间: ${nd.expected_range_pct||'-'} | 方向偏向: ${nd.direction_bias||'-'} | 置信度: ${nd.confidence||'-'}%</div>
        </div>`;
      if(nd.scenarios)nd.scenarios.forEach(s=>{
        let sc=s.change.startsWith('+')?'color:var(--rd)':'color:var(--gn)';
        h+=`<div style="padding:6px 10px;background:var(--bg);border-radius:6px;margin-bottom:4px;display:flex;justify-content:space-between;align-items:center">
          <span style="font-size:.8rem;font-weight:600">${s.name} <span style="font-size:.7rem;color:var(--tx2)">${s.probability}</span></span>
          <span style="font-weight:600;${sc}">${s.price} (${s.change})</span>
          <span style="font-size:.7rem;color:var(--tx2)">${s.desc||''}</span></div>`;
      });
      h+=`<div style="font-size:.78rem;color:var(--tx2);margin-top:6px;line-height:1.4">📊 ${nd.summary||''}</div>`;
      if(nd.reasons)h+=`<div style="margin-top:6px">${nd.reasons.map(r=>`<span style="display:inline-block;margin:2px 4px;padding:2px 8px;background:rgba(88,166,255,.08);border-radius:8px;font-size:.7rem">${r}</span>`).join('')}</div>`;
      h+=`</div>`;
    }
    // 日内做T分析
    let t0=d.t0_trading||{};
    if(t0.pivot){
      h+=`<div style="background:linear-gradient(135deg,rgba(63,185,80,.06),rgba(248,81,73,.06));border:1px solid rgba(63,185,80,.2);border-radius:10px;padding:16px;margin-top:16px">
        <h4 style="margin:0 0 10px;font-size:.85rem;color:var(--gn)">📊 ${t0.mode==='intraday'?'盘中做T·当日':'盘后做T·次日'} (T+0) <span style="font-size:.7rem;color:var(--tx2)">基于${t0.ref_label||''}数据</span></h4>
        <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-bottom:10px">
          <div style="text-align:center;background:var(--bg);border-radius:8px;padding:8px"><div style="font-size:.7rem;color:var(--tx2)">枢轴P</div><div style="font-weight:700">${t0.pivot}</div></div>
          <div style="text-align:center;background:var(--bg);border-radius:8px;padding:8px"><div style="font-size:.7rem;color:var(--rd)">阻力R1/R2</div><div style="font-weight:700;color:var(--rd)">${t0.r1}/${t0.r2}</div></div>
          <div style="text-align:center;background:var(--bg);border-radius:8px;padding:8px"><div style="font-size:.7rem;color:var(--gn)">支撑S1/S2</div><div style="font-weight:700;color:var(--gn)">${t0.s1}/${t0.s2}</div></div>
          <div style="text-align:center;background:var(--bg);border-radius:8px;padding:8px"><div style="font-size:.7rem;color:var(--tx2)">做T空间/风险</div><div><span style="color:var(--gn)">+${t0.t0_space}%</span>/<span style="color:var(--rd)">-${t0.t0_risk}%</span></div></div>
        </div>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:8px">
          <div><div style="font-weight:600;font-size:.8rem;color:var(--gn);margin-bottom:4px">📥 做T买入点</div>`;
      if(t0.buy_points)t0.buy_points.slice(0,3).forEach(p=>h+=`<div style="font-size:.72rem;padding:3px 0;border-bottom:1px solid var(--bd)"><b>${p.price}</b> - ${p.label} <span style="color:var(--tx2)">${p.desc}</span></div>`);
      h+=`</div><div><div style="font-weight:600;font-size:.8rem;color:var(--rd);margin-bottom:4px">📤 做T卖出点</div>`;
      if(t0.sell_points)t0.sell_points.slice(0,3).forEach(p=>h+=`<div style="font-size:.72rem;padding:3px 0;border-bottom:1px solid var(--bd)"><b>${p.price}</b> - ${p.label} <span style="color:var(--tx2)">${p.desc}</span></div>`);
      h+=`</div></div>
        <div style="background:rgba(163,113,247,.08);border-radius:8px;padding:10px;font-size:.8rem;line-height:1.5">💡 ${t0.advice||''}</div>
      </div>`;
    }
    // 综合总结
    h+=`<div style="background:linear-gradient(135deg,rgba(88,166,255,.1),rgba(163,113,247,.1));border:1px solid rgba(88,166,255,.3);border-radius:10px;padding:16px;margin-top:12px">
      <h4 style="margin:0 0 8px;font-size:.85rem">📝 综合总结</h4>
      <p style="font-size:.9rem;line-height:1.6;margin:0">${rec.summary||''}</p>
      <p style="font-size:.78rem;color:var(--tx2);margin-top:8px">${sent.summary||''}</p></div>`;
    body.innerHTML=h;
  }catch(e){body.innerHTML=`<div class="fp">请求失败: ${e.message}</div>`}
}
// 回车触发分析
document.getElementById('stockCode').addEventListener('keydown',function(e){if(e.key==='Enter')analyzeOne()});

// ======= 持仓分析 =======
async function analyzePos(){
  let code=document.getElementById('posCode').value.trim();
  let mkt=document.getElementById('posMkt').value;
  let price=parseFloat(document.getElementById('posPrice').value);
  let shares=parseInt(document.getElementById('posShares').value);
  let target=parseFloat(document.getElementById('posTarget').value)||null;
  if(!code||!price||!shares){alert('请填写完整的持仓信息');return}
  let res=document.getElementById('posResult');
  res.innerHTML='<div style="text-align:center;padding:20px"><div style="width:20px;height:20px;border:3px solid var(--bd);border-top-color:var(--pr);border-radius:50%;animation:spin .6s linear infinite;display:inline-block"></div><p style="margin-top:8px;color:var(--tx2)">正在分析持仓...</p></div>';
  try{
    let body={code:code,market:mkt,buy_price:price,shares:shares};if(target)body.target_amount=target;
    let r=await fetch('/api/position',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    let d=await r.json();
    if(!d.success){res.innerHTML=`<div class="fp"><span class="et">分析失败:</span> ${d.message}</div>`;return}
    let rs=d.result;let pnlCls=rs.pnl>=0?'color:var(--rd)':'color:var(--gn)';
    let h=`<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:12px;margin-bottom:16px">
      <div style="background:var(--bg);border-radius:8px;padding:12px"><div style="font-size:.75rem;color:var(--tx2)">${rs.name||rs.code}</div><div style="font-size:1.3rem;font-weight:700">${rs.current_price} <small style="${pnlCls}">${rs.pnl>=0?'+':''}${rs.pnl_pct}%</small></div></div>
      <div style="background:var(--bg);border-radius:8px;padding:12px"><div style="font-size:.75rem;color:var(--tx2)">持仓成本/市值</div><div style="font-size:1.3rem;font-weight:700">${rs.cost} / ${rs.current_value}</div></div>
      <div style="background:var(--bg);border-radius:8px;padding:12px"><div style="font-size:.75rem;color:var(--tx2)">浮动盈亏</div><div style="font-size:1.3rem;font-weight:700;${pnlCls}">${rs.pnl>=0?'+':''}${rs.pnl.toFixed(0)}</div></div>
      <div style="background:var(--bg);border-radius:8px;padding:12px"><div style="font-size:.75rem;color:var(--tx2)">股数/均价</div><div style="font-size:1.2rem;font-weight:700">${rs.shares}股 / ${rs.buy_price}</div></div>
    </div>`;
    // 综合建议
    h+=`<div style="background:linear-gradient(135deg,rgba(163,113,247,.1),rgba(88,166,255,.1));border:1px solid rgba(163,113,247,.3);border-radius:10px;padding:14px;margin-bottom:14px">
      <h4 style="margin:0 0 6px;font-size:.85rem;color:var(--pr)">💡 综合建议</h4>
      <p style="font-size:.88rem;line-height:1.5;margin:0">${rs.advice||''}</p></div>`;
    // 策略列表
    let strats=rs.strategies||[];
    if(strats.length>0){
      // 按类型分组
      let groups={};
      strats.forEach(s=>{let k=s.type;if(!groups[k])groups[k]=[];groups[k].push(s)});
      for(let[type,items]of Object.entries(groups)){
        let icon=type.includes('盈利')||type.includes('止盈')?'📈':type.includes('加仓')?'📥':type.includes('止损')?'🛑':'📊';
        h+=`<div style="background:var(--bg);border-radius:10px;padding:14px;margin-bottom:10px"><h4 style="margin:0 0 10px;font-size:.82rem;color:var(--bl)">${icon} ${type}方案</h4>`;
        items.forEach(s=>{
          h+=`<div style="border:1px solid var(--bd);border-radius:8px;padding:10px;margin-bottom:8px">`;
          if(s.target)h+=`<div style="font-weight:700;font-size:.85rem">目标: ${s.target} → ${s.target_price}元 | 盈利${s.target_profit?.toFixed(0)||'-'}元</div>`;
          if(s.add_shares!==undefined)h+=`<div style="font-weight:700;font-size:.85rem">加仓${s.add_shares}股(约${s.add_amount?.toFixed(0)||'-'}元) → 新均价${s.new_avg_cost}元</div>`;
          if(s.sell_shares!==undefined)h+=`<div style="font-weight:700;font-size:.85rem">卖出${s.sell_shares}股@${s.sell_price} → 锁定利润${s.profit_locked?.toFixed(0)||'-'}元</div>`;
          if(s.stop_price)h+=`<div style="font-weight:700;font-size:.85rem;color:var(--rd)">止损价: ${s.stop_price}元 | 最大亏损${s.stop_loss?.toFixed(0)||'-'}元</div>`;
          h+=`<div style="font-size:.8rem;color:var(--tx2);margin-top:4px">📌 ${s.action||''}</div>`;
          if(s.time_note)h+=`<div style="font-size:.75rem;color:var(--or);margin-top:2px">⏱ ${s.time_note}</div>`;
          h+=`</div>`;
        });
        h+=`</div>`;
      }
    }
    // 路线图
    let route=rs.route_plan;
    if(route&&route.nodes&&route.nodes.length>0){
      h+=`<div style="background:linear-gradient(135deg,rgba(227,179,65,.08),rgba(88,166,255,.08));border:1px solid rgba(227,179,65,.3);border-radius:10px;padding:14px;margin-top:10px">
        <h4 style="margin:0 0 4px;font-size:.85rem;color:var(--yl)">🗺️ 达到目标盈利{route.target_amount?.toFixed(0)||'-'}元的操作路线图</h4>
        <p style="font-size:.78rem;color:var(--tx2);margin-bottom:10px">${route.summary||''}</p>
        <div style="position:relative;padding-left:20px">`;
      route.nodes.forEach((n,i)=>{
        let icon=n.action==='加仓'?'📥':n.action==='减仓止盈'?'📤':'📌';
        let clr=n.direction==='up'?'var(--rd)':'var(--gn)';
        h+=`<div style="position:relative;padding:8px 0 8px 20px;border-left:2px solid ${clr};margin-left:6px">
          <div style="position:absolute;left:-8px;top:10px;width:14px;height:14px;border-radius:50%;background:${clr};border:2px solid var(--bg)"></div>
          <div style="font-weight:700;font-size:.85rem">${icon} ${n.trigger}<span style="color:${clr}">${n.price}元</span> → ${n.action} <span style="color:var(--bl)">${n.shares}股</span></div>
          <div style="font-size:.76rem;color:var(--tx2);margin-top:2px">${n.desc||''}</div>
          <div style="font-size:.72rem;color:var(--or);margin-top:1px">⏱ ${n.time||''}</div>
          ${n.progress_pct!==undefined?`<div style="font-size:.72rem;color:var(--gn);margin-top:1px">✅ 完成进度${n.progress_pct}%</div>`:''}
          ${n.avg_after!==undefined?`<div style="font-size:.72rem;color:var(--tx2)">📊 操作后: 均价${n.avg_after}元 | 持仓${n.total_after}股</div>`:''}
        </div>`;
      });
      h+=`</div></div>`;
    }
    res.innerHTML=h;
  }catch(e){res.innerHTML=`<div class="fp">请求失败: ${e.message}</div>`}
}

// 全局错误捕获
window.onerror=function(msg,url,line){console.error('JS Error:',msg,'line',line);document.getElementById('errs').innerHTML='<div class="fp">页面脚本错误: '+msg+' (行'+line+')<br>请重启程序后刷新</div>';return false};
ss('ok','就绪');
</script></body></html>"""

if __name__ == '__main__':
    import os
    port = int(os.environ.get('PORT', 5000))
    print(f"""
╔══════════════════════════════════════════════════════╗
║  📊 股票技术形态自动扫描系统 v5.2                   ║
║  8种形态 + 情绪因子(换手率/量比/连阳/振幅/涨速)    ║
║  访问: http://localhost:{port}                        ║
║  先"测试200只"验证 → 再"全量扫描"                  ║
╚══════════════════════════════════════════════════════╝
""")
    init_baostock()
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
