from dataclasses import dataclass, asdict
import datetime
@dataclass
class PartnerData:
    date: datetime.date
    dsp_id: int = 0
    ssp: str = ''
    imps: int = 0
    spent: float = 0
    currency: str = 'usd'

@dataclass
class PartnerData2:
    date: datetime.date = None
    dsp_id: int = 0
    ssp: str = ''
    imps: int = 0
    spent: float = 0
    dsp_reqs: int = 0
    dsp_resp: int = 0
    clicks: int =0
    view100: int =0
    currency: str = 'usd'
