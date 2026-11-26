import requests,time,hashlib,json,re
from urllib.parse import urlparse,urljoin
from datetime import date,datetime
from decimal import Decimal,InvalidOperation
from defusedxml import ElementTree as ET
X='http://www.xbrl.org/2003/instance'
MAP={}
def register(statement, pairs):
    for code,tags in pairs.items():
        for tag in tags.split(): MAP[tag]=(statement,code)
register('PNL',{'REVENUE':'RevenueFromOperations TotalRevenueFromOperations','INTEREST_EARNED':'InterestEarned InterestIncome','OTHER_INCOME':'OtherIncome','FEE_INCOME':'FeesAndCommissionIncome','TOTAL_INCOME':'Income TotalIncome','FINANCE_COST':'FinanceCosts InterestExpended','DEPRECIATION':'DepreciationDepletionAndAmortisationExpense','PROVISIONS':'ProvisionsAndContingencies ImpairmentOnFinancialInstruments','TOTAL_EXPENSES':'Expenses','EXPENSES_EX_PROVISIONS':'ExpenditureExcludingProvisionsAndContingencies','PBT':'ProfitBeforeTax ProfitLossFromOrdinaryActivitiesBeforeTax','TAX':'TaxExpense','NET_PROFIT':'ProfitLossForPeriod','BASIC_EPS':'BasicEarningsLossPerShareFromContinuingAndDiscontinuedOperations BasicEarningsPerShareBeforeExtraordinaryItems','DILUTED_EPS':'DilutedEarningsLossPerShareFromContinuingAndDiscontinuedOperations DilutedEarningsPerShareBeforeExtraordinaryItems','EMPLOYEE_EXPENSE':'EmployeeBenefitsExpense','MATERIAL_COST':'CostOfMaterialsConsumed','OTHER_EXPENSE':'OtherExpenses'})
register('BS',{'ASSETS':'Assets','CURRENT_ASSETS':'CurrentAssets','NONCURRENT_ASSETS':'NoncurrentAssets','EQUITY':'Equity','CAPITAL_RESERVES':'CapitalAndReserves','LIABILITIES':'Liabilities','EQUITY_LIABILITIES':'EquityAndLiabilities CapitalAndLiabilities','CASH':'CashAndCashEquivalents','RBI_CASH':'CashAndBalancesWithReserveBankOfIndia','CURRENT_BORROWINGS':'BorrowingsCurrent','NONCURRENT_BORROWINGS':'BorrowingsNoncurrent','BORROWINGS':'Borrowings','DEPOSITS':'Deposits','ADVANCES':'Advances','LOANS':'Loans','DEBT_SECURITIES':'DebtSecurities','INVENTORY':'Inventories','RECEIVABLES':'TradeReceivables','CURRENT_LIABILITIES':'CurrentLiabilities','NONCURRENT_LIABILITIES':'NoncurrentLiabilities'})
register('CF',{'CFO':'CashFlowsFromUsedInOperatingActivities','CFI':'CashFlowsFromUsedInInvestingActivities','CFF':'CashFlowsFromUsedInFinancingActivities','CAPEX':'PurchaseOfPropertyPlantAndEquipmentClassifiedAsInvestingActivities','NET_CHANGE_BEFORE_FX':'IncreaseDecreaseInCashAndCashEquivalentsBeforeEffectOfExchangeRateChanges','NET_CHANGE':'IncreaseDecreaseInCashAndCashEquivalents','FX':'EffectOfExchangeRateChangesOnCashAndCashEquivalents','CASH_BALANCE':'CashAndCashEquivalentsCashFlowStatement'})
def local(s): return s.split('}')[-1].split(':')[-1]
def scope(s):
    s=re.sub('[^a-z]','',str(s).lower())
    return {'consolidated':'CONSOLIDATED','standalone':'STANDALONE','nonconsolidated':'STANDALONE','consolidatedmember':'CONSOLIDATED','standalonemember':'STANDALONE'}.get(s,'UNKNOWN')
def period(start,end):
    if not start:return 'INSTANT'
    n=(date.fromisoformat(end)-date.fromisoformat(start)).days+1
    return next((k for a,b,k in [(80,100,'QUARTERLY'),(170,195,'HALF_YEARLY'),(260,285,'NINE_MONTHS'),(350,375,'ANNUAL')] if a<=n<=b),'OTHER')
def parse(raw, filing_scope):
    root=ET.fromstring(raw)
    if root.tag!=f'{{{X}}}xbrl':raise ValueError('Not an XBRL instance')
    contexts={}; units={}; issues=[]; facts=[]
    for c in root.findall(f'{{{X}}}context'):
        p=c.find(f'{{{X}}}period'); start=p.findtext(f'{{{X}}}startDate'); end=p.findtext(f'{{{X}}}endDate') or p.findtext(f'{{{X}}}instant'); sc=filing_scope; dims=[]
        for e in c.iter():
            if local(e.tag) in ('explicitMember','typedMember'):
                axis=e.get('dimension',''); member=local(e.text or '')
                if 'consolidatedandstandalone' in axis.lower():
                    s=scope(member); sc=s if sc in ('UNKNOWN',s) else 'UNKNOWN'
                else:dims.append(ET.tostring(e,encoding='unicode'))
        contexts[c.get('id')]=(start,end,sc,json.dumps(sorted(dims)),c.findtext(f'.//{{{X}}}identifier'))
    for u in root.findall(f'{{{X}}}unit'):
        measures=[local(e.text or '') for e in u.iter() if local(e.tag)=='measure']; units[u.get('id')]='INR' if measures==['INR'] else 'INR/share' if measures==['INR','shares'] else '/'.join(measures)
    for e in root:
        if not e.get('contextRef') or not e.get('unitRef') or e.get('{http://www.w3.org/2001/XMLSchema-instance}nil') in ('true','1') or not (e.text or '').strip():continue
        try:
            start,end,sc,dims,entity=contexts[e.get('contextRef')]; date.fromisoformat(end); pt=period(start,end); v=Decimal(e.text.strip()); assert v.is_finite(); unit=units[e.get('unitRef')]; assert unit
            tag=local(e.tag); statement,code=MAP.get(tag,('UNMAPPED',e.tag)); status='CANDIDATE' if statement!='UNMAPPED' and sc!='UNKNOWN' and ((statement=='BS')==(pt=='INSTANT') or code=='CASH_BALANCE') else 'UNMAPPED_OR_AMBIGUOUS'
            if code.endswith('EPS') and unit!='INR/share':status='UNMAPPED_OR_AMBIGUOUS'
            if statement!='UNMAPPED' and not code.endswith('EPS') and unit!='INR':status='UNMAPPED_OR_AMBIGUOUS'
            facts.append(dict(tag=e.tag,statement=statement,code=code,start=start or '',end=end,period=pt,scope=sc,dimensions=dims,entity=entity,unit=unit,value=format(v,'f'),decimals=e.get('decimals'),status=status))
        except (ValueError,KeyError,TypeError,AssertionError,InvalidOperation) as ex:issues.append({'tag':e.tag,'error':str(ex),'value':e.text})
    return facts,issues
class NSE:
    def __init__(self):
        self.session=requests.Session(); self.session.headers.update({'User-Agent':'Financial research ETL/2.0','Accept':'application/json, text/plain, */*','Accept-Language':'en-US,en;q=0.9'}); self.last=0; self.blocked=False
    def get(self,url,params=None):
        if self.blocked:raise RuntimeError('Source circuit open')
        for attempt in range(4):
            target=url; query=params
            for _ in range(5):
                p=urlparse(target)
                if p.scheme!='https' or p.hostname not in {'www.nseindia.com','www.nseindia.in','nsearchives.nseindia.com','nsearchives.nseindia.in'} or p.username or p.password or p.path.lower().endswith(('/null','/none','/undefined')):raise ValueError('Invalid source URL')
                time.sleep(max(0,1.2-(time.monotonic()-self.last))); self.last=time.monotonic()
                r=self.session.get(target,params=query,timeout=45,allow_redirects=False,stream=True)
                if r.is_redirect:target=urljoin(r.url,r.headers['Location']); query=None; r.close(); continue
                break
            else:raise RuntimeError('Too many redirects')
            with r:
                if r.status_code in (401,403):self.blocked=True; raise RuntimeError(f'Access denied {r.status_code}')
                if r.status_code==429 or r.status_code>=500:
                    delay=r.headers.get('Retry-After',''); time.sleep(min(120,max(2**attempt,int(delay) if delay.isdigit() else 0))); continue
                r.raise_for_status(); data=bytearray()
                for chunk in r.iter_content(65536):
                    data.extend(chunk)
                    if len(data)>40_000_000:raise ValueError('Document too large')
                return bytes(data)
        raise RuntimeError('Retry budget exhausted')
    def discover(self,symbol,year,route):
        params={'index':'equities','symbol':symbol}
        if route!='annual-reports':params.update(from_date=f'01-01-{year}',to_date=f'31-12-{year}')
        if route=='integrated-filing-results':params.update(type='Integrated Filing- Financials',size=20,page=1)
        periods=['Quarterly','Half-Yearly','Annual'] if route in ('financial-results','corporates-financial-results') else [None]
        for p in periods:
            q=dict(params)
            if p:q['period']=p
            count=0
            for page in range(1,101):
                if route=='integrated-filing-results':q['page']=page
                raw=self.get('https://www.nseindia.com/api/'+route,q); obj=json.loads(raw); rows=obj if isinstance(obj,list) else obj.get('data');
                if not isinstance(rows,list):raise ValueError('Unexpected response schema')
                yield raw,rows,q
                count+=len(rows); total=obj.get('totalCount') if isinstance(obj,dict) else None
                if total is None or count>=int(total):break
                if not rows or route!='integrated-filing-results':raise ValueError('Incomplete pagination')
            else:raise ValueError('Pagination limit')
