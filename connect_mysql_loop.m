% first connect to MySQL data base

mydb = database('betfair_full','ivan','m0bxksQd','Vendor','MySQL','Server','localhost');

%% End database connect

NUMBER_OF_RUNNERS = 6;

sqlquery = 'SELECT * FROM 6RUNNERS_RACE_LIST_W_DATES';

% generic list as a query result
df_dataframe1 = exec(mydb, sqlquery);

Markets_obj = fetch(df_dataframe1); % in R is dbFetch

Markets = Markets_obj.Data; % to extract real data from the object!!

RaceId = Markets{1,1}; %% cell type needs a bit of attention

RaceDate = Markets{1,2}; %% date casted from cell type

%%

sqlquery1 = ['SELECT DISTINCT HORSENAME FROM 6RUNNERS_MARKETS_BSP WHERE MARKETID_2 = ' num2str(RaceId)];

df_dataframe2 = exec(mydb, sqlquery1); 
runners_obj = fetch(df_dataframe2);
runners = runners_obj.data; % all runners in a race

%% time series extract for every runner

%% Define synchronous dummy data

sqlquerytv = ['SELECT DISTINCT(CAST(TIME_STAMP AS CHAR(11))) FROM 6RUNNERS_MARKETS_BSP WHERE MARKETID_2 = ' num2str(RaceId) ' ORDER BY TIME_STAMP'];
df_timevector = exec(mydb,sqlquerytv);
timevector_obj = fetch(df_timevector);
timevector = timevector_obj.data;


timestart = datenum(timevector{1});

[tv_dim_r tv_dim_c] = size(timevector);

timeend = datenum(timevector{tv_dim_r});

dt = datenum('00:00:00.020')-datenum('00:00:00.010');

dt_cutoff = datenum('00:30:00') - datenum('00:00:00.010');

t_cutoffstart = timeend - dt_cutoff;

sync_timevector = timestart:dt:timeend;
sync_timevector = sync_timevector';

dummydata = NaN(size(sync_timevector));

dummyts = timeseries(dummydata',sync_timevector');

%% Define synchronous dummy data END


for i = 1:NUMBER_OF_RUNNERS
	q = char(39); %% important for quotes expected by MySQL
	horsename = strcat(q, runners{i}, q);

% use SELECT CAST(TIME_STAMP AS CHAR(11)) since normal sql query returns
% truncated time

	sqlquery2 = ['SELECT CAST(TIME_STAMP AS CHAR(11)), HORSENAME, BSP FROM 6RUNNERS_MARKETS_BSP WHERE MARKETID_2 = ' num2str(RaceId) ' AND HORSENAME= ' horsename ' ORDER BY HORSENAME, TIME_STAMP'];

	df_dataframe3 = exec(mydb, sqlquery2);  
	price_obj = fetch(df_dataframe3);
	price = price_obj.data; % all price movements - type cell

% convert to numerical vectors
	ts = (price(1:end,1)); % has to remain cell type coloumn vector; strips fractions of a second
	ts_ms = strcat(ts,'0'); % add zero to convert to miliseconds

	BSP = cell2mat(price(1:end, 3));
% DO NOT USE FOR TIME SERIES!!!!!!!!!!
% inp_str_format = 'HH:MM:SS.FFF'; 
% ts_ms_num = datenum(ts_ms,inp_str_format);
% DO NOT USE FOR TIME SERIES!!!!!!!!!!

	ts_ms_num = datenum(ts_ms);
	BSP_ts = timeseries(BSP',ts_ms_num'); % work with time series objects not fints (part of financial toolbox)

% WATCH OUT - NON COMMUTATIVE
	[ts1 BSP_sync_ts] = synchronize(dummyts,BSP_ts,'Union'); % output vector BSP_sync_ts is synchronized


% Cutoff (extract) the last portion of the market trading
	BSP_sync_ts_cutoff = getsampleusingtime(BSP_sync_ts, t_cutoffstart,timeend);
	plot(BSP_sync_ts_cutoff);


% Append the time series in the collection
	switch i
		case 1 
		BSP_ts_collection = BSP_sync_ts_cutoff;
		case 2
		BSP_ts_collection = tscollection(BSP_ts_collection,BSP_sync_ts_cutoff);
		otherwise
		BSP_ts_collection = addts(BSP_ts_collection,BSP_sync_ts_cutoff)
	end % switch

end % for

X = BSP_ts_collection.Data; % this is probably OK for matrix extraction 
% see getdatasamples function


% [Mu,Cov,P,Pi,LL]=fhmm(X,T,M,K,cyc,tol)

% First Try












%%

close(mydb);
