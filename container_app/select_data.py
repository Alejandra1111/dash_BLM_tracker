import boto3
import json


import conf_credentials as conf

# aws lambda clinent 
client = boto3.client('lambda',
                        region_name= conf.region,
                        aws_access_key_id=conf.aws_access_key_id,
                        aws_secret_access_key=conf.aws_secret_access_key)



''' factory method for selecting stats etc. 
'''

def assess_cases(
    stat_type, stat_triggered_context, date, filter_keyword, hour, city,
    latest_datatime_d_dt, latest_datatime_hour): 

    if stat_type!='current stats' and stat_triggered_context=='picked_hour':
        # skip to showing stats for the selected hour
        print('skipping picked_stats update')
        case = 'skip'

    elif str(date)[:10]==str(latest_datatime_d_dt)[:10] and filter_keyword=='' and str(hour)==str(latest_datatime_hour):
        # load current stats 
        print('Returning current stats for ' + city)
        case = 'current_stats'

    elif stat_triggered_context=='filter_submit' and filter_keyword!='':
        # filter data and re-calculate stats
        print('Getting stats for ' + city + ' on ' + date[:10] + ' with filter: ', filter_keyword)
        case = 'filtered_city_date'

    else:
        # retrieve city_date stats 
        print('Getting stats for ' + city + ' on ' + date[:10])
        case = 'city_date'
    return case


class CityDateStats:
    def __init__(self, city, date, filter_keyword, current_data_cities):
        self.data = {
            'city': city, 'date': date[:10], 
            'filter_keyword': filter_keyword,
            'city_data': current_data_cities[city]
            }

    def add_properties(self, selector, case):
        selector.add_data(self.data)
        selector.add_case_property(case)


class SelectStatsCurrent:
    def add_data(self, data):
        self._city_data = data['city_data'] 

    def add_case_property(self, case):
        pass

    def select(self):
        stats = self._city_data
        stats['type'] = 'current stats'
        return stats


class SelectStatsAWS:
    def add_data(self, data):
        self._data = data 

    def add_case_property(self, case):
        self._func = 'BLM_get_stats' if case=='city_date' else 'BLM_stats'

    def select(self):
        result = client.invoke(FunctionName=self._func,
                        InvocationType='RequestResponse',                                      
                        Payload=json.dumps(self._data))
        print(result)
        return json.loads(json.loads(result['Payload'].read()))



def stat_list(stats):
    return [stats['stat_sentiments'], stats['stat_emotions'], stats['stat_words'], 
        stats['top_tweets'], stats['top_users'], stats['type']]




class SelectorFactory():
    def __init__(self):
        self._creators = {}

    def register_selector(self, case, creator):
        self._creators[case] = creator

    def get_selector(self, case):
        creator = self._creators.get(case)
        if not creator:
            raise ValueError(case)
        return creator() 

factory = SelectorFactory()
factory.register_selector('current_stats', SelectStatsCurrent)
factory.register_selector('city_date', SelectStatsAWS)
factory.register_selector('filtered_city_date', SelectStatsAWS)



class ObjectSelector:
    def select(self, item, case):
        selector = factory.get_selector(case)
        item.add_properties(selector, case)
        return selector.select()

# use example:
# case = assess_cases(stat_type, stat_triggered_context, date, filter_keyword, hour, city)
# selector = ObjectSelector()
# stats = selector.select(CityDateStats(city, date, filter_keyword), case)

