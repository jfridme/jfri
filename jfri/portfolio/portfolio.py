
class Asset:
    def __init__(this, portfolio, code):
        this.portfolio = portfolio
        this.id = portfolio.create_asset_id()
        this.open_time = portfolio.trading_time
        this.code = code
        return

    def __repr__(this):
        return f"Asset({this.code}, {this.open_time}, {this.open_price}, {this.open_amount})"

    def __str__(this):
        return f"{this.code} {this.open_time} {this.open_price} {this.open_amount}"

    def open(this, price, amount):
        '''
        资产开仓

        price: float | None
            开仓价
            如果为None，则使用市场数据获取当前价
        amount: float
            开仓额度
        '''
        if price is None:
            price = this.portfolio.market_data.get_price(this.portfolio.trading_time, this.code)
            if price is None:
                return 0
        this.open_price = price
        this.open_amount = amount
        return amount

    def close(this, price, amount):
        '''
        资产平仓

        price: float | None
            平仓价
            如果为None，则使用市场数据获取当前价
        amount: float
            预期平仓额度
        return:
            实际平仓额度
        '''
        if price is None:
            price = this.portfolio.market_data.get_price(this.portfolio.trading_time, this.code)
            if price is None:
                return 0

        open_amount_to_close = this.open_amount / this.open_price * price

        if amount is not None and open_amount_to_close > amount:
            this.open_amount -= amount / price * this.open_price
            return amount
        else:
            this.open_amount = 0
            return open_amount_to_close

    def empty(this):
        '''
        资产是否已全部平仓

        return: bool
        '''
        return this.open_amount <= 0

    def get_value(this):
        '''
        资产市值

        return: float
            资产市值
        '''
        price = this.portfolio.market_data.get_price(this.portfolio.trading_time, this.code)
        if price is None:
            return this.open_amount
        else:
            return this.open_amount / this.open_price * price


class MarketData_pl:
    def __init__(this, price_pl):
        '''
        初始化市场数据

        price_pl: Polars DataFrame
            价格数据
            'time'列为时间戳，其他列为品种代码，每行代表一个时间点的价格

            数据示例：
┌───────────┬───────────┬───────────┬───────────┬───┬───────────┬───────────┬───────────┬──────────┐
│ time      ┆ 600000.SH ┆ 600001.SH ┆ 600003.SH ┆ … ┆ 301607.SZ ┆ 301608.SZ ┆ 301611.SZ ┆ 301618.S │
│ ---       ┆ ---       ┆ ---       ┆ ---       ┆   ┆ ---       ┆ ---       ┆ ---       ┆ Z        │
│ i64       ┆ f64       ┆ f64       ┆ f64       ┆   ┆ f64       ┆ f64       ┆ f64       ┆ ---      │
│           ┆           ┆           ┆           ┆   ┆           ┆           ┆           ┆ f64      │
╞═══════════╪═══════════╪═══════════╪═══════════╪═══╪═══════════╪═══════════╪═══════════╪══════════╡
│ 142038720 ┆ 16.07     ┆ 0.0       ┆ 0.0       ┆ … ┆ 0.0       ┆ 0.0       ┆ 0.0       ┆ 0.0      │
│ 0000      ┆           ┆           ┆           ┆   ┆           ┆           ┆           ┆          │
│ 142047360 ┆ 16.13     ┆ 0.0       ┆ 0.0       ┆ … ┆ 0.0       ┆ 0.0       ┆ 0.0       ┆ 0.0      │
│ 0000      ┆           ┆           ┆           ┆   ┆           ┆           ┆           ┆          │
│ 142056000 ┆ 15.81     ┆ 0.0       ┆ 0.0       ┆ … ┆ 0.0       ┆ 0.0       ┆ 0.0       ┆ 0.0      │
│ 0000      ┆           ┆           ┆           ┆   ┆           ┆           ┆           ┆          │
│ 142064640 ┆ 15.25     ┆ 0.0       ┆ 0.0       ┆ … ┆ 0.0       ┆ 0.0       ┆ 0.0       ┆ 0.0      │
│ 0000      ┆           ┆           ┆           ┆   ┆           ┆           ┆           ┆          │
│ 142073280 ┆ 15.43     ┆ 0.0       ┆ 0.0       ┆ … ┆ 0.0       ┆ 0.0       ┆ 0.0       ┆ 0.0      │
│ 0000      ┆           ┆           ┆           ┆   ┆           ┆           ┆           ┆          │
│ …         ┆ …         ┆ …         ┆ …         ┆ … ┆ …         ┆ …         ┆ …         ┆ …        │
│ 172702080 ┆ 8.63      ┆ 0.0       ┆ 0.0       ┆ … ┆ 31.49     ┆ 51.3      ┆ 27.52     ┆ 0.0      │
│ 0000      ┆           ┆           ┆           ┆   ┆           ┆           ┆           ┆          │
│ 172710720 ┆ 9.09      ┆ 0.0       ┆ 0.0       ┆ … ┆ 32.23     ┆ 51.81     ┆ 29.1      ┆ 0.0      │
│ 0000      ┆           ┆           ┆           ┆   ┆           ┆           ┆           ┆          │
│ 172719360 ┆ 9.49      ┆ 0.0       ┆ 0.0       ┆ … ┆ 31.92     ┆ 51.94     ┆ 30.62     ┆ 0.0      │
│ 0000      ┆           ┆           ┆           ┆   ┆           ┆           ┆           ┆          │
│ 172728000 ┆ 9.84      ┆ 0.0       ┆ 0.0       ┆ … ┆ 32.36     ┆ 51.9      ┆ 32.0      ┆ 0.0      │
│ 0000      ┆           ┆           ┆           ┆   ┆           ┆           ┆           ┆          │
│ 172736640 ┆ 9.87      ┆ 0.0       ┆ 0.0       ┆ … ┆ 35.17     ┆ 57.85     ┆ 34.72     ┆ 0.0      │
│ 0000      ┆           ┆           ┆           ┆   ┆           ┆           ┆           ┆          │
└───────────┴───────────┴───────────┴───────────┴───┴───────────┴───────────┴───────────┴──────────┘
        '''
        this.price = price_pl
        this.cache = None
        this.cache_time = None
        return

    def get_price(this, time, code):
        '''
        获取指定品种在指定时间的价格

        time: any 和market_data.time_list中的元素类型一致
            时间
        code: str
            品种代码
        return: float
            价格
        '''
        if this.cache_time != time:
            this.cache = this.price.filter(this.price['time'] == time)
            this.cache_time = time

        stock_data = this.cache[code]
        if stock_data.shape[0] > 0:
            return stock_data[-1]
        else:
            return None


class MarketData_pd:
    def __init__(this, price_pd):
        '''
        初始化市场数据

        price_pd: Pandas DataFrame
            价格数据
            'time'列为时间戳，其他列为品种代码，每行代表一个时间点的价格
        '''
        this.price = price_pd
        this.cache = None
        this.cache_time = None
        return

    def get_price(this, time, code):
        '''
        获取指定品种在指定时间的价格

        time: any 和market_data.time_list中的元素类型一致
            时间
        code: str
            品种代码
        return: float
            价格
        '''
        if this.cache_time != time:
            this.cache = this.price[this.price.index == time]
            this.cache_time = time

        stock_data = this.cache[code]
        if stock_data.shape[0] > 0:
            return stock_data.iloc[-1]
        else:
            return None


class Portfolio:
    '''
    资产组合
    '''

    asset_num = 0

    def __init__(this, market_data):
        '''
        初始化投资组合

        market_data: 满足 MarketData 接口的对象
            市场数据
        '''
        this.assets = {}
        this.asset_map = {}
        this.market_data = market_data
        this.trading_time = None
        return

    def set_trading_time(this, trading_time):
        '''
        设置交易日

        trading_time: any 和market_data.time_list中的元素类型一致
            交易日
        '''
        this.trading_time = trading_time
        return

    def create_asset_id(this):
        '''
        创建资产ID

        return: int
            资产ID
        '''
        Portfolio.asset_num += 1
        return Portfolio.asset_num

    def open_asset(this, code, price, amount):
        '''
        资产开仓

        code: str
            品种代码
        price: float | None
            开仓价
            如果为None，则使用市场数据获取当前价
        amount: float
            开仓额度
        '''
        asset = Asset(this, code)
        amount = asset.open(price, amount)
        if amount <= 0:
            return 0

        this.assets[asset.id] = asset
        if code in this.asset_map:
            this.asset_map[code].append(asset)
        else:
            this.asset_map[code] = [asset]
        return amount

    def close_asset(this, code, price, amount):
        '''
        资产平仓

        code: str
            品种代码
        price: float | None
            平仓价
            如果为None，则使用市场数据获取当前价
        amount: float | None
            预期平仓额度
            如果为None，则全部平仓
        return:
            实际平仓额度
        '''
        asset_list = this.asset_map.get(code, [])
        closed_amount = 0

        if amount is None:
            ai = 0
            while ai < len(asset_list):
                asset = asset_list[ai]
                closed_amount += asset.close(price, None)
                if asset.empty():
                    this.assets.pop(asset.id)
                    asset_list.pop(ai)
                else:
                    ai += 1
        else:
            ai = 0
            while closed_amount < amount and ai < len(asset_list):
                asset = asset_list[ai]
                closed_amount += asset.close(price, amount - closed_amount)
                if asset.empty():
                    this.assets.pop(asset.id)
                    asset_list.pop(ai)
                else:
                    ai += 1
        return closed_amount

    def get_asset(this, code):
        '''
        获取指定品种的资产列表

        code: str
            品种代码
        return: dict
            资产市值字典
        '''
        asset_list = this.asset_map.get(code, [])
        value = sum(asset.get_value() for asset in asset_list)
        return {code: value}

    def get_all_assets(this):
        '''
        获取所有资产市值字典

        return: dict
            资产市值字典
        '''
        return {code: sum(asset.get_value() for asset in asset_list) for code, asset_list in this.asset_map.items()}

    def get_portfolio_value(this):
        '''
        获取整个投资组合的市值

        return: float
            整个投资组合的市值
        '''
        return sum(asset.get_value() for asset in this.assets.values())
