import datetime
import gc
import numpy as np
import pandas as pd
import pymysql
import os
import threading
import time
import statsmodels.api as sm

fundamental_field_list = ['CURFDS','SETTRESEDEPO','PLAC','TRADFINASSET', 'DERIFINAASSET',
            'NOTESRECE', 'ACCORECE', 'PREP','PREMRECE', 'REINRECE', 'REINCONTRESE', 'INTERECE', 'DIVIDRECE',
           'OTHERRECE', 'EXPOTAXREBARECE', 'SUBSRECE', 'MARGRECE', 'INTELRECE',
           'PURCRESAASSET', 'INVE', 'ACCHELDFORS', 'PREPEXPE', 'UNSEG',
           'EXPINONCURRASSET', 'OTHERCURRASSE', 'TOTCURRASSET', 'LENDANDLOAN',
           'AVAISELLASSE', 'HOLDINVEDUE', 'LONGRECE', 'EQUIINVE', 'OTHERLONGINVE',
           'INVEPROP', 'FIXEDASSEIMMO', 'ACCUDEPR', 'FIXEDASSENETW', 'FIXEDASSEIMPA',
           'FIXEDASSENET', 'CONSPROG', 'ENGIMATE', 'FIXEDASSECLEA', 'PRODASSE',
           'COMASSE', ' HYDRASSET', 'INTAASSET', 'DEVEEXPE', 'GOODWILL',
           'LOGPREPEXPE', ' TRADSHARTRAD', 'DEFETAXASSET', 'OTHERNONCASSE',
           'TOTALNONCASSETS', 'TOTASSET', 'SHORTTERMBORR', 'CENBANKBORR',
           'DEPOSIT', 'FDSBORR', 'TRADFINLIAB', 'DERILIAB', 'NOTESPAYA',
           'ACCOPAYA', 'ADVAPAYM', 'SELLREPASSE', 'COPEPOUN', 'COPEWORKERSAL',
           'TAXESPAYA', 'INTEPAYA', 'DIVIPAYA', 'OTHERFEEPAYA', 'MARGREQU',
           'INTELPAY', 'OTHERPAY', 'ACCREXPE', 'EXPECURRLIAB', 'COPEWITHREINRECE',
           'INSUCONTRESE', 'ACTITRADSECU', ' ACTIUNDESECU', 'INTETICKSETT',
           'DOMETICKSETT', 'DEFEREVE', ' SHORTTERMBDSPAYA', 'LIABHELDFORS',
           'DUENONCLIAB', 'OTHERCURRELIABI', ' OTALCURRLIAB', 'LONGBORR',
           'LCOPEWORKERSAL', 'BDSPAYA', 'BDSPAYAPREST', 'BDSPAYAPERBOND',
           'LONGPAYA', 'SPECPAYA', 'EXPENONCLIAB', 'LONGDEFEINCO',
           'DEFEINCOTAXLIAB', 'OTHERNONCLIABI', 'TOTALNONCLIAB', 'TOTLIAB',
           'PAIDINCAPI', 'OTHEQUIN', 'PREST', 'PERBOND', 'CAPISURP', 'TREASTK',
           'OCL', ' SPECRESE', 'RESE', 'GENERISKRESE', 'UNREINVELOSS', 'UNDIPROF',
           'TOPAYCASHDIVI', 'CURTRANDIFF', 'PARESHARRIGH', 'MINYSHARRIGH',
           'RIGHAGGR', 'TOTLIABSHAREQUI', 'WARLIABRESE', 'NOTESACCORECE',
           'CONTRACTASSET', 'OTHDEBTINVEST', 'OTHEQUININVEST', 'OTHERNONCFINASSE',
           'NOTESACCOPAYA', 'CONTRACTLIAB', 'FAIRVALUEASSETS', 'AMORTIZCOSTASSETS']

fundamental_id_to_statement = dict((con, 'TQ_FIN_PROBALSHEETNEW') for con in fundamental_field_list)


class Localthread(threading.Thread):
    '''
        需要修改host等参数
    '''
    def __init__(self, func, args, filepath, keep='first', name=''):
        threading.Thread.__init__(self)
        self.func = func
        self.args = args
        self.name = name
        self.filepath = filepath
        self.keep = keep

    def run(self):
        db = pymysql.connect(host='183.174.228.97',
                             port= 3306,
                             user='ruc_tongji',
                             password='tongji_bigdata!',
                             database='fcdb')
        cursor = db.cursor()
        query_str = self.func(*self.args)
        cursor.execute(query_str)
        print(f'Running Thread:{threading.Thread.getName(self)}')
        field_names = [con[0] for con in cursor.description]
        data = cursor.fetchall()
        data = pd.DataFrame(list(data),columns=field_names)
        try:
            data = data.drop_duplicates(['report_date','stk_code'], keep=self.keep)
        except KeyError:
            data = data.drop_duplicates(['stk_code'], keep=self.keep)
        data = data.set_index(['trade_date', 'st_code'])
        data.to_csv(self.filepath + '\\' + self.name)


    #Tools
    def __Datafile(self, fields, start_date = None, end_date = None):

        file_list = os.listdir(self.filepath)
        filename = [file.split('.')[0] for file in file_list]
        com_field = list(set(fields) & set(filename))

        if len(com_field) > 0:
            def _get_start_date(index, start_date=start_date, end_date = end_date):
                if len(index)>0: #防止出现空csv的情况
                    data_start_date = str(min(index))
                    if data_start_date > start_date:
                        data_start_date = self.date_transform(pd.to_datetime(data_start_date) - datetime.timedelta(days=1))[0]
                        return [start_date, data_start_date]
                    else:
                        return []
                else:
                    return [start_date, end_date]

            def _get_end_date(index,end_date=end_date):
                if len(index)>0:
                    data_end_date = str(max(index))
                    if data_end_date < end_date:
                        data_end_date = self.date_transform(pd.to_datetime(data_end_date) + datetime.timedelta(days=1))[0]
                        return [data_end_date, end_date]
                    else:
                        return []
                else:
                    return [] #因为此时_get_start_date 一定会出来，留一个就行了

            idxList = []
            for file in com_field:
                try:
                    idxList.append(pd.read_csv(self.filepath + '\\' + file + '.csv',index_col=0).index.values)
                except FileNotFoundError:
                    pass

            startArgs = [[com_field[idx]] + _get_start_date(idxList[idx]) for idx in range(len(idxList))]
            endArgs = [[com_field[idx]] + _get_end_date(idxList[idx]) for idx in range(len(idxList))]
            del idxList
            gc.collect()

            Args = sorted([con for con in (startArgs + endArgs) if len(con)==3])
        else:
            Args = []

        need_field = set(fields) - set(com_field)

        for field in need_field:
            Args.append((field, start_date, end_date))

        return (tuple(Args), com_field, need_field)


    def thread_fetch_data(self, type, fields, start_date, end_date):

        st = time.time()

        if len(fields) > 0:
            (args, com_field, need_field) = self.__Datafile(fields, start_date, end_date)
            Threads = []

            for arg in args:
                name = str(arg[0])+'_'+str(arg[1])+'_'+str(arg[2])
                td = Localthread(func=eval('self._get_{}_sql'.format(type)), args=arg, filepath = self.filepath,name=name)
                Threads.append(td)
                Threads[-1].start()

            for idx in range(len(args)):
                Threads[idx].join()

            print('Data Fetching Complete, Time: ', time.time()-st)


    def _Data_combine(self, filelist):
        #Notes: we have three csv file at most. If there is more than Three files, check your filepath
        if len(filelist) > 3:
            raise ValueError('More than three files, Data Pollution Maybe')
        data = []
        for file in filelist:
            data_tmp = pd.read_csv(self.filepath + '\\'+file, index_col = 0)
            os.remove(self.filepath + '\\'+file)
            data.append(data_tmp)

        if len(data)>1:
            data = pd.concat(data,axis=0)
        else:
            data = data[0]

        data = data.reset_index()
        data = data.sort_values(['trade_date'])
        data = data.drop_duplicates(['trade_date'], keep='last')
        data = data.set_index(['trade_date','st_code'])

        try: del data['index']
        except: pass

        filename = filelist[0].split('_')[0]
        data.to_csv(self.filepath + '\\' + filename.split('.')[0] + '.csv' )
        gc.collect()
        print('{} has been overwritten'.format(filename))
        return data


    def _collect_filelist(self, fields):
        file_list = os.listdir(self.filepath)
        data = []
        for field in fields:
            data_tmp = []
            for file in file_list:
                if field in file:
                    data_tmp.append(file)
            data.append(data_tmp)
        return data


    @staticmethod
    def date_transform(*args):
        return pd.to_datetime(args).strftime("%Y%m%d").values

    @staticmethod
    def _get_fundamental_sql(field, start_date, end_date):
        print('Starting fetching {field}, Start date: {st}, End date:{ed}'.format(field=field,
                                                                                  st=start_date, ed=end_date))
        statement_id = fundamental_id_to_statement[field]
        query_str = '''
                        SELECT {base}.PUBLISHDATE AS trade_date, {base}.ENDDATE AS report_date, 
                        TQ_OA_STCODE.SYMBOL AS st_code, {field} AS {field}
                        FROM {base}
                        RIGHT JOIN TQ_OA_STCODE ON {base}.COMPCODE=TQ_OA_STCODE.COMPCODE
                        WHERE TQ_OA_STCODE.SETYPE = '101'
                        AND {base}.PUBLISHDATE >= {start_date}
                        AND {base}.PUBLISHDATE <= {end_date}
                        AND {base}.REPORTTYPE = 1 | 3
                    '''.format(base=statement_id, field = field, start_date = start_date, end_date = end_date)
        return query_str

    def get_data(self,market_fields=[], fundamental_fields=[], filter_fields=[], start_date=None, end_date=None):
        start_date, end_date = self.date_transform(start_date, end_date)

        if start_date is None:
            start_date = '19700101'
        if end_date is None:
            end_date = datetime.datetime.now().strftime("%Y%m%d")

        if len(market_fields)>0:
            self.thread_fetch_data('market', fields=market_fields, start_date = start_date,
                                         end_date = end_date)

        if len(fundamental_fields)>0:
            self.thread_fetch_data('fundamental', fields=fundamental_fields,start_date = start_date,
                                         end_date = end_date)

        if len(filter_fields)>0:
            self.thread_fetch_data('filter', fields=filter_fields, start_date=start_date,
                                       end_date=end_date)

        fields = fundamental_fields + market_fields + filter_fields
        file_list = self._collect_filelist(fields)
        data_list = []
        for file in file_list:
            data_list.append(self._Data_combine(file))
        return pd.concat(data_list,axis=1)
