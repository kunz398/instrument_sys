--
-- PostgreSQL database dump
--

-- Dumped from database version 17.5
-- Dumped by pg_dump version 17.0

-- Started on 2026-07-09 10:53:15

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

DROP DATABASE instrument_db;
--
-- TOC entry 4918 (class 1262 OID 65622)
-- Name: instrument_db; Type: DATABASE; Schema: -; Owner: postgres
--

CREATE DATABASE instrument_db WITH TEMPLATE = template0 ENCODING = 'UTF8' LOCALE_PROVIDER = libc LOCALE = 'English_Australia.1252';


ALTER DATABASE instrument_db OWNER TO postgres;

\connect instrument_db

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- TOC entry 217 (class 1259 OID 65623)
-- Name: access_method; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.access_method (
    id integer NOT NULL,
    function character varying
);


ALTER TABLE public.access_method OWNER TO postgres;

--
-- TOC entry 218 (class 1259 OID 65628)
-- Name: access_method_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.access_method_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.access_method_id_seq OWNER TO postgres;

--
-- TOC entry 4919 (class 0 OID 0)
-- Dependencies: 218
-- Name: access_method_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.access_method_id_seq OWNED BY public.access_method.id;


--
-- TOC entry 219 (class 1259 OID 65629)
-- Name: station; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.station (
    id integer NOT NULL,
    description character varying,
    station_id character varying,
    latitude double precision,
    longitude double precision,
    owner character varying,
    maintainer character varying,
    is_active boolean,
    variable_id character varying,
    variable_label character varying,
    project character varying,
    type_id integer,
    access_method_id integer,
    status_id integer,
    country_id integer,
    source_url character varying,
    token_id integer,
    intervals double precision DEFAULT 0,
    bad_data character varying,
    display_name character varying,
    data_limit integer DEFAULT 100,
    mean double precision DEFAULT 0,
    chart_type character varying(50) DEFAULT 'line'::character varying NOT NULL
);


ALTER TABLE public.station OWNER TO postgres;

--
-- TOC entry 4920 (class 0 OID 0)
-- Dependencies: 219
-- Name: COLUMN station.status_id; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.station.status_id IS 'This column represents the internal status of the row. It indicates the state of the record within the application (e.g., deleted or inactive or active), and is not related to the actual station data.
--
this controls if a station will appear on the front end or not';


--
-- TOC entry 4921 (class 0 OID 0)
-- Dependencies: 219
-- Name: COLUMN station.bad_data; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.station.bad_data IS 'Comma-separated string containing bad data values';


--
-- TOC entry 4922 (class 0 OID 0)
-- Dependencies: 219
-- Name: COLUMN station.display_name; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.station.display_name IS 'display name for stations';


--
-- TOC entry 220 (class 1259 OID 65638)
-- Name: station_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.station_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.station_id_seq OWNER TO postgres;

--
-- TOC entry 4923 (class 0 OID 0)
-- Dependencies: 220
-- Name: station_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.station_id_seq OWNED BY public.station.id;


--
-- TOC entry 221 (class 1259 OID 65639)
-- Name: status; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.status (
    id integer NOT NULL,
    value character varying
);


ALTER TABLE public.status OWNER TO postgres;

--
-- TOC entry 222 (class 1259 OID 65644)
-- Name: status_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.status_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.status_id_seq OWNER TO postgres;

--
-- TOC entry 4924 (class 0 OID 0)
-- Dependencies: 222
-- Name: status_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.status_id_seq OWNED BY public.status.id;


--
-- TOC entry 223 (class 1259 OID 65645)
-- Name: token; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.token (
    id integer NOT NULL,
    token character varying NOT NULL,
    comments character varying
);


ALTER TABLE public.token OWNER TO postgres;

--
-- TOC entry 224 (class 1259 OID 65650)
-- Name: token_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.token_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.token_id_seq OWNER TO postgres;

--
-- TOC entry 4925 (class 0 OID 0)
-- Dependencies: 224
-- Name: token_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.token_id_seq OWNED BY public.token.id;


--
-- TOC entry 225 (class 1259 OID 65651)
-- Name: type; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.type (
    id integer NOT NULL,
    value character varying
);


ALTER TABLE public.type OWNER TO postgres;

--
-- TOC entry 226 (class 1259 OID 65656)
-- Name: type_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.type_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.type_id_seq OWNER TO postgres;

--
-- TOC entry 4926 (class 0 OID 0)
-- Dependencies: 226
-- Name: type_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.type_id_seq OWNED BY public.type.id;


--
-- TOC entry 4715 (class 2604 OID 65657)
-- Name: access_method id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.access_method ALTER COLUMN id SET DEFAULT nextval('public.access_method_id_seq'::regclass);


--
-- TOC entry 4716 (class 2604 OID 65658)
-- Name: station id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.station ALTER COLUMN id SET DEFAULT nextval('public.station_id_seq'::regclass);


--
-- TOC entry 4721 (class 2604 OID 65659)
-- Name: status id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.status ALTER COLUMN id SET DEFAULT nextval('public.status_id_seq'::regclass);


--
-- TOC entry 4722 (class 2604 OID 65660)
-- Name: token id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.token ALTER COLUMN id SET DEFAULT nextval('public.token_id_seq'::regclass);


--
-- TOC entry 4723 (class 2604 OID 65661)
-- Name: type id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.type ALTER COLUMN id SET DEFAULT nextval('public.type_id_seq'::regclass);


--
-- TOC entry 4903 (class 0 OID 65623)
-- Dependencies: 217
-- Data for Name: access_method; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.access_method (id, function) FROM stdin;
3	dart_method
1	spot_method
2	pacioos_method
4	ioc_method
5	neon_method
6	pacioos_smart_method
\.


--
-- TOC entry 4905 (class 0 OID 65629)
-- Dependencies: 219
-- Data for Name: station; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.station (id, description, station_id, latitude, longitude, owner, maintainer, is_active, variable_id, variable_label, project, type_id, access_method_id, status_id, country_id, source_url, token_id, intervals, bad_data, display_name, data_limit, mean, chart_type) FROM stdin;
186	 NEWPORT - 210NM West of Coos Bay, OR 	46407	42.71	-128.89	NDBC	\N	t	time,m	time,sea_level (m)	\N	3	3	1	0	https://www.ndbc.noaa.gov/dart_data.php?station=STATION_ID&startmonth=START_MONTH&startday=START_DAY&startyear=START_YEAR&endmonth=END_MONTH&endday=END_DAY&endyear=END_YEAR	\N	0	\N	 NEWPORT - 210NM West of Coos Bay, OR 	100	-3267.13	line
181	 GULF OF AMERICA - 247 NM South of New Orleans, LA 	42409	25.8	-89.29	NDBC	\N	t	time,m	time,sea_level (m)	\N	3	3	1	0	https://www.ndbc.noaa.gov/dart_data.php?station=STATION_ID&startmonth=START_MONTH&startday=START_DAY&startyear=START_YEAR&endmonth=END_MONTH&endday=END_DAY&endyear=END_YEAR	\N	0	\N	 GULF OF AMERICA - 247 NM South of New Orleans, LA 	100	-3199.62	line
182	 SOUTHWEST MANZANILLO - 240 NM SW of Manzanillo, MX 	43412	16.02	-107	NDBC	\N	t	time,m	time,sea_level (m)	\N	3	3	1	0	https://www.ndbc.noaa.gov/dart_data.php?station=STATION_ID&startmonth=START_MONTH&startday=START_DAY&startyear=START_YEAR&endmonth=END_MONTH&endday=END_DAY&endyear=END_YEAR	\N	0	\N	 SOUTHWEST MANZANILLO - 240 NM SW of Manzanillo, MX 	100	-3098.26	line
183	 SOUTH ACAPULCO - 360NM South of Acapulco, MX 	43413	10.93	-100.01	NDBC	\N	t	time,m	time,sea_level (m)	\N	3	3	1	0	https://www.ndbc.noaa.gov/dart_data.php?station=STATION_ID&startmonth=START_MONTH&startday=START_DAY&startyear=START_YEAR&endmonth=END_MONTH&endday=END_DAY&endyear=END_YEAR	\N	0	\N	 SOUTH ACAPULCO - 360NM South of Acapulco, MX 	100	-3425.34	line
184	 SOUTHEAST BLOCK CANYON - 130 NM SE of Fire Island, NY 	44402	39.31	-70.72	NDBC	\N	t	time,m	time,sea_level (m)	\N	3	3	1	0	https://www.ndbc.noaa.gov/dart_data.php?station=STATION_ID&startmonth=START_MONTH&startday=START_DAY&startyear=START_YEAR&endmonth=END_MONTH&endday=END_DAY&endyear=END_YEAR	\N	0	\N	 SOUTHEAST BLOCK CANYON - 130 NM SE of Fire Island, NY 	100	-2605.12	line
185	 SABLE ISLAND BANK - 437 NM E of Boston, MA 	44403	41.93	-61.66	NDBC	\N	t	time,m	time,sea_level (m)	\N	3	3	1	0	https://www.ndbc.noaa.gov/dart_data.php?station=STATION_ID&startmonth=START_MONTH&startday=START_DAY&startyear=START_YEAR&endmonth=END_MONTH&endday=END_DAY&endyear=END_YEAR	\N	0	\N	 SABLE ISLAND BANK - 437 NM E of Boston, MA 	100	-3899.1	line
187	 Mendocino - 150 NM West of  Mendocino Bay, CA 	46411	39.34	-127.04	NDBC	\N	t	time,m	time,sea_level (m)	\N	3	3	1	0	https://www.ndbc.noaa.gov/dart_data.php?station=STATION_ID&startmonth=START_MONTH&startday=START_DAY&startyear=START_YEAR&endmonth=END_MONTH&endday=END_DAY&endyear=END_YEAR	\N	0	\N	 Mendocino - 150 NM West of  Mendocino Bay, CA 	100	-4266.45	line
178	 NORTH ST THOMAS - 300 NM North of St Thomas, Virgin Is 	41421	23.41	-63.89	NDBC	\N	t	time,m,lon_deg,lat_deg	time,water column HEIGHT,lon_deg,lat_deg	\N	3	3	2	0	https://www.ndbc.noaa.gov/dart_data.php?station=STATION_ID&startmonth=START_MONTH&startday=START_DAY&startyear=START_YEAR&endmonth=END_MONTH&endday=END_DAY&endyear=END_YEAR	\N	0	\N	\N	100	0	line
163	219	219	7.629632	134.6704	PACIOOS	\N	t	time,waveHs,waveTp,waveDp	time,significant_wave_height,peak_wave_period,peak_wave_direction	\N	1	2	2	10	https://erddap.cdip.ucsd.edu/erddap/tabledap/wave_agg.geoJson?station_id,time,waveHs,waveTp,waveTa,waveDp,latitude,longitude&station_id=%22STATION_ID%22&time%3E=START_TIME&time%3C=END_TIME&waveFlagPrimary=1	\N	0	\N	\N	100	0	line
191	 NZC - Offshore Gisborne Hikurangi 	5401000	-38.2	-179.8	New Zealand National Emergency Management Agency by Te Pu Ao GNS Science and NIWA Taihoro Nukurangi	\N	t	time,m	time,sea_level (m)	\N	3	3	1	0	https://www.ndbc.noaa.gov/dart_data.php?station=STATION_ID&startmonth=START_MONTH&startday=START_DAY&startyear=START_YEAR&endmonth=END_MONTH&endday=END_DAY&endyear=END_YEAR	\N	0	\N	 NZC - Offshore Gisborne Hikurangi 	100	-3545.47	line
197	 NZA - Offshore Wellington Hikurangi 	5501002	-42.37	176.91	New Zealand National Emergency Management Agency by Te Pu Ao GNS Science and NIWA Taihoro Nukurangi	\N	t	time,m	time,sea_level (m)	\N	3	3	1	0	https://www.ndbc.noaa.gov/dart_data.php?station=STATION_ID&startmonth=START_MONTH&startday=START_DAY&startyear=START_YEAR&endmonth=END_MONTH&endday=END_DAY&endyear=END_YEAR	\N	0	\N	 NZA - Offshore Wellington Hikurangi 	100	-2634.77	line
198	 NZD - Offshore Bay of Plenty Kermadec 	5501004	-36.1	178.6	New Zealand National Emergency Management Agency by Te Pu Ao GNS Science and NIWA Taihoro Nukurangi	\N	t	time,m	time,sea_level (m)	\N	3	3	1	0	https://www.ndbc.noaa.gov/dart_data.php?station=STATION_ID&startmonth=START_MONTH&startday=START_DAY&startyear=START_YEAR&endmonth=END_MONTH&endday=END_DAY&endyear=END_YEAR	\N	0	\N	 NZD - Offshore Bay of Plenty Kermadec 	100	-2447.68	line
210	auct	auct	-36.8314	174.7865	Land Information New Zealand ( New Zealand )	Land Information New Zealand ( New Zealand )	t	slevel,stime	sea_level (m),time	\N	4	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&includesensors%5B%5D=rad&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	New Zealand, Auckland NZ - Tide Gauge 	1000	0	line
222	chuu	chuu	7.4536833	151.8982417	Korea-South Pacific Ocean Research Center ( Korea )	Korea-South Pacific Ocean Research Center ( Korea )	t	slevel,stime	sea_level (m),time	\N	4	4	2	5	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&includesensors%5B%5D=rad&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Micronesia, Chuuk - Tide Gauge 	100	0	line
196	 NZI - Offshore Samoa Tonga     	5401005	-16.89	-171.19	New Zealand National Emergency Management Agency by Te Pu Ao GNS Science and NIWA Taihoro Nukurangi	\N	t	time,m	time,sea_level (m)	\N	3	3	1	0	https://www.ndbc.noaa.gov/dart_data.php?station=STATION_ID&startmonth=START_MONTH&startday=START_DAY&startyear=START_YEAR&endmonth=END_MONTH&endday=END_DAY&endyear=END_YEAR	\N	0	\N	 NZI - Offshore Samoa Tonga     	100	-5237.11	line
284	kapi	kapi	1.0779583	154.8066861	National Weather Service ( Micronesia )	National Weather Service ( Micronesia )	t	slevel,stime	sea_level (m),time	\N	4	4	2	5	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&includesensors%5B%5D=rad&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Mirconesia, Kapingamarangi FM - Tide Gauge	100	0	line
308	ofuas	ofuas	-14.163364	-169.681033	University of Hawaii Sea Level Center ( USA )	University of Hawaii Sea Level Center ( USA )	t	slevel,stime	sea_level (m),time	\N	4	4	1	18	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&includesensors%5B%5D=rad&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Samoa, Ofu - Tide Gauge	1000	0	line
313	penr	penr	-9.001	-158.051	University of Hawaii Sea Level Center ( USA )	University of Hawaii Sea Level Center ( USA )	t	slevel,stime	sea_level (m),time	\N	4	4	2	16	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&includesensors%5B%5D=rad&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	0	Cook Is, Penrhyn CK - Tide Gauge	100	0	line
335	tauas	tauas	-14.239845	-169.510648	University of Hawaii Sea Level Center ( USA )	University of Hawaii Sea Level Center ( USA )	t	slevel,stime	sea_level (m),time	\N	4	4	1	18	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&includesensors%5B%5D=rad&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Samoa, Tau - Tide Gauge 	1000	0	line
201	 NZL - Offshore Vanuatu New Hebrides 	5501007	-19.31	166.78	New Zealand National Emergency Management Agency by Te Pu Ao GNS Science and NIWA Taihoro Nukurangi	\N	t	time,m,lon_deg,lat_deg	time,water column HEIGHT,lon_deg,lat_deg	\N	3	3	2	0	https://www.ndbc.noaa.gov/dart_data.php?station=STATION_ID&startmonth=START_MONTH&startday=START_DAY&startyear=START_YEAR&endmonth=END_MONTH&endday=END_DAY&endyear=END_YEAR	\N	0	\N	\N	100	0	line
362	upol	upol	-13.8268	-171.7613	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time		4	4	1	12	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END	8	0		Samoa, Apia - Tide Gauge	1000	0	line
363	mala	mala	7.3282	134.4502	National Weather Service ( Micronesia )	National Weather Service ( Micronesia )	t	slevel,stime	sea_level (m),time	\N	4	4	1	10	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&&includesensors%5B%5D=rad&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Palau, Malakal - Tide Gauge	1000	0	line
364	yap	yapi	9.5142	138.1246	National Weather Service ( Micronesia )	National Weather Service ( Micronesia )	t	slevel,stime	sea_level (m),time	\N	4	4	1	5	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&&includesensors%5B%5D=rad&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Yap Island - Tide Gauge	1000	0	line
179	 SOUTHWEST BERMUDA - 200 NM SSW of Hamilton, Bermuda 	41425	28.64	-65.77	NDBC	\N	t	time,m	time,sea_level (m)	\N	3	3	1	0	https://www.ndbc.noaa.gov/dart_data.php?station=STATION_ID&startmonth=START_MONTH&startday=START_DAY&startyear=START_YEAR&endmonth=END_MONTH&endday=END_DAY&endyear=END_YEAR	\N	0	\N	 SOUTHWEST BERMUDA - 200 NM SSW of Hamilton, Bermuda 	100	-5222.27	line
128	SPOT-0403	SPOT-0403	-9.42227	159.94742	SPC	\N	t	significantWaveHeight,peakPeriod,peakDirection,timestamp	significant_wave_height,peak_wave_period,peak_wave_direction,time	\N	1	1	1	13	https://wavefleet.spoondriftspotter.co/api/wave-data?spotterId=SPOT-0403&token=REPALCE_TOKEN_STRING&includeWaves=true&includeTrack=true&includeDirectionalMoments=true&limit=500	1	0	\N	Solomon Islands, Wavebuoy	100	0	line
195	 NZH - Offshore Niue Tonga      	5401004	-20.09	-171.86	New Zealand National Emergency Management Agency by Te Pu Ao GNS Science and NIWA Taihoro Nukurangi	\N	t	time,m,lon_deg,lat_deg	time,water column HEIGHT,lon_deg,lat_deg	\N	3	3	2	0	https://www.ndbc.noaa.gov/dart_data.php?station=STATION_ID&startmonth=START_MONTH&startday=START_DAY&startyear=START_YEAR&endmonth=END_MONTH&endday=END_DAY&endyear=END_YEAR	\N	0	\N	\N	100	0	line
168	SPOT-31091C	SPOT-31091C	-19.05455	-169.9315	NMS	\N	f	significantWaveHeight,peakPeriod,peakDirection,timestamp	significant_wave_height,peak_wave_period,peak_wave_direction,time	\N	1	1	2	9	https://wavefleet.spoondriftspotter.co/api/wave-data?spotterId=SPOT-31091C&token=REPALCE_TOKEN_STRING&includeWaves=true&includeTrack=true&includeDirectionalMoments=true&limit=500	7	0	\N	Niue, Alofi, Wavebuoy	100	0	line
129	SPOT-0435	SPOT-0435	-21.74347	165.335	SPC	\N	f	significantWaveHeight,peakPeriod,peakDirection,timestamp	significant_wave_height,peak_wave_period,peak_wave_direction,time	\N	1	1	1	20	https://wavefleet.spoondriftspotter.co/api/wave-data?spotterId=SPOT-0435&token=REPALCE_TOKEN_STRING&includeWaves=true&includeTrack=true&includeDirectionalMoments=true&limit=500	1	0	\N	Noumea, Wavebuoy	100	0	line
159	SPOT-0303	SPOT-0303	-8.591	179.05518	TMS	\N	t	significantWaveHeight,peakPeriod,peakDirection,timestamp	significant_wave_height,peak_wave_period,peak_wave_direction,time	\N	1	1	1	4	https://wavefleet.spoondriftspotter.co/api/wave-data?spotterId=SPOT-0303&token=REPALCE_TOKEN_STRING&includeWaves=true&includeTrack=true&includeDirectionalMoments=true&limit=500	6	0	\N	Tuvalu, Funafuti, Wavebuoy	100	0	line
161	178	178	7.081486	158.2442	PACIOOS	\N	t	time,waveHs,waveTp,waveDp	time,significant_wave_height,peak_wave_period,peak_wave_direction	\N	1	2	1	5	https://erddap.cdip.ucsd.edu/erddap/tabledap/wave_agg.geoJson?station_id,time,waveHs,waveTp,waveTa,waveDp,latitude,longitude&station_id=%22STATION_ID%22&time%3E=START_TIME&time%3C=END_TIME&waveFlagPrimary=1	\N	0	\N	RMI, Wavebuoy	100	0	line
162	196	196	13.6833	144.8155	PACIOOS	\N	t	time,waveHs,waveTp,waveDp	time,significant_wave_height,peak_wave_period,peak_wave_direction	\N	1	2	1	24	https://erddap.cdip.ucsd.edu/erddap/tabledap/wave_agg.geoJson?station_id,time,waveHs,waveTp,waveTa,waveDp,latitude,longitude&station_id=%22STATION_ID%22&time%3E=START_TIME&time%3C=END_TIME&waveFlagPrimary=1	\N	0	\N	Guam, Wavebuoy	100	0	line
164	273	273	-14.2965	-170.8747	PACIOOS	\N	t	time,waveHs,waveTp,waveDp	time,significant_wave_height,peak_wave_period,peak_wave_direction	\N	1	2	1	18	https://erddap.cdip.ucsd.edu/erddap/tabledap/wave_agg.geoJson?station_id,time,waveHs,waveTp,waveTa,waveDp,latitude,longitude&station_id=%22STATION_ID%22&time%3E=START_TIME&time%3C=END_TIME&waveFlagPrimary=1	\N	0	\N	American Samoa, Wavebuoy	100	0	line
169	121	121	13.3555	144.788	PACIOOS	\N	t	time,waveHs,waveTp,waveDp	time,significant_wave_height,peak_wave_period,peak_wave_direction	\N	1	2	1	24	https://erddap.cdip.ucsd.edu/erddap/tabledap/wave_agg.geoJson?station_id,time,waveHs,waveTp,waveTa,waveDp,latitude,longitude&station_id=%22STATION_ID%22&time%3E=START_TIME&time%3C=END_TIME&waveFlagPrimary=1	\N	0	\N	Guam, Wavebuoy	100	0	line
138	SPOT-0301	SPOT-0301	1.33658	173.01658	KMS	\N	f	significantWaveHeight,peakPeriod,peakDirection,timestamp	significant_wave_height,peak_wave_period,peak_wave_direction,time	\N	1	1	1	6	https://wavefleet.spoondriftspotter.co/api/wave-data?spotterId=SPOT-0301&token=REPALCE_TOKEN_STRING&includeWaves=true&includeTrack=true&includeDirectionalMoments=true&limit=500	3	0	\N	Kiribati, Tarawa, Wavebuoy	100	0	line
158	SPOT-0302	SPOT-0302	-6.29783	176.3046	TMS	\N	f	significantWaveHeight,peakPeriod,peakDirection,timestamp	significant_wave_height,peak_wave_period,peak_wave_direction,time	\N	1	1	1	4	https://wavefleet.spoondriftspotter.co/api/wave-data?spotterId=SPOT-0302&token=REPALCE_TOKEN_STRING&includeWaves=true&includeTrack=true&includeDirectionalMoments=true&limit=500	6	0	\N	Tuvalu, Nanumaga, Wavebuoy	100	0	line
160	SPOT-1411	SPOT-1411	-8.59382	179.05637	TMS	\N	f	significantWaveHeight,peakPeriod,peakDirection,timestamp	significant_wave_height,peak_wave_period,peak_wave_direction,time	\N	1	1	1	4	https://wavefleet.spoondriftspotter.co/api/wave-data?spotterId=SPOT-1411&token=REPALCE_TOKEN_STRING&includeWaves=true&includeTrack=true&includeDirectionalMoments=true&limit=500	6	0	\N	Tuvalu, Funafuti, Wavebuoy	100	0	line
133	SPOT-30355R	SPOT-30355R	-21.7311	165.30062	SPC	\N	f	significantWaveHeight,peakPeriod,peakDirection,timestamp	significant_wave_height,peak_wave_period,peak_wave_direction,time	\N	1	1	1	20	https://wavefleet.spoondriftspotter.co/api/wave-data?spotterId=SPOT-30355R&token=REPALCE_TOKEN_STRING&includeWaves=true&includeTrack=true&includeDirectionalMoments=true&limit=500	1	0	\N	Noumea, Wavebuoy	100	0	line
365	Niue Buoy	SPOT-30979C	-19.0543333	190.0702833	SPC		t	significantWaveHeight,peakPeriod,peakDirection,timestamp	significant_wave_height,peak_wave_period,peak_wave_direction,time		1	1	1	9	https://wavefleet.spoondriftspotter.co/api/wave-data?spotterId=SPOT-30979C&token=2a348598f294c6b0ce5f7e41e5c0f5&includeWaves=true&limit=500	5	0		SPOT-30979C	1000	0	line
135	SPOT-0596	SPOT-0596	-19.06755	177.98447	FMS	\N	f	significantWaveHeight,peakPeriod,peakDirection,timestamp	significant_wave_height,peak_wave_period,peak_wave_direction,time	\N	1	1	1	2	https://wavefleet.spoondriftspotter.co/api/wave-data?spotterId=SPOT-0596&token=REPALCE_TOKEN_STRING&includeWaves=true&includeTrack=true&includeDirectionalMoments=true&limit=500	2	0	\N	Fiji, Kadavu, Wavebuoy	100	0	line
136	SPOT-0607	SPOT-0607	-18.95167	178.37548	FMS	\N	f	significantWaveHeight,peakPeriod,peakDirection,timestamp	significant_wave_height,peak_wave_period,peak_wave_direction,time	\N	1	1	1	2	https://wavefleet.spoondriftspotter.co/api/wave-data?spotterId=SPOT-0607&token=REPALCE_TOKEN_STRING&includeWaves=true&includeTrack=true&includeDirectionalMoments=true&limit=500	2	0	\N	Fiji, Kadavu, Wavebuoy	100	0	line
156	SPOT-1612	SPOT-1612	-23.02458	-175.02327	TongaMet	\N	f	significantWaveHeight,peakPeriod,peakDirection,timestamp	significant_wave_height,peak_wave_period,peak_wave_direction,time	\N	1	1	1	3	https://wavefleet.spoondriftspotter.co/api/wave-data?spotterId=SPOT-1612&token=REPALCE_TOKEN_STRING&includeWaves=true&includeTrack=true&includeDirectionalMoments=true&limit=500	5	0	\N	Tonga, Wavebuoy	100	0	line
154	SPOT-1951	SPOT-1951	-21.24395	-175.1372	TongaMet	\N	f	significantWaveHeight,peakPeriod,peakDirection,timestamp	significant_wave_height,peak_wave_period,peak_wave_direction,time	\N	1	1	1	3	https://wavefleet.spoondriftspotter.co/api/wave-data?spotterId=SPOT-1951&token=REPALCE_TOKEN_STRING&includeWaves=true&includeTrack=true&includeDirectionalMoments=true&limit=500	5	0	\N	Tonga, Wavebuoy	100	0	line
157	SPOT-30032R	SPOT-30032R	-21.12362	-175.19375	TongaMet	\N	f	significantWaveHeight,peakPeriod,peakDirection,timestamp	significant_wave_height,peak_wave_period,peak_wave_direction,time	\N	1	1	1	3	https://wavefleet.spoondriftspotter.co/api/wave-data?spotterId=SPOT-30032R&token=REPALCE_TOKEN_STRING&includeWaves=true&includeTrack=true&includeDirectionalMoments=true&limit=500	5	0	\N	Tonga, Wavebuoy	100	0	line
166	SPOT-31153C	SPOT-31153C	-18.9747	-169.9024667	NMS	\N	f	significantWaveHeight,peakPeriod,peakDirection,timestamp	significant_wave_height,peak_wave_period,peak_wave_direction,time	\N	1	1	1	9	https://wavefleet.spoondriftspotter.co/api/wave-data?spotterId=SPOT-31153C&token=REPALCE_TOKEN_STRING&includeWaves=true&includeTrack=true&includeDirectionalMoments=true&limit=500	7	0	\N	Niue, Wavebuoy	100	0	line
4	This Returns Waves data	SPOT-31071C	-19.06193	-169.98748	SPC	SPC	t	significantWaveHeight,peakPeriod,peakDirection,timestamp	significant_wave_height,peak_wave_period,peak_wave_direction,time	instrument	1	1	2	2	https://wavefleet.spoondriftspotter.co/api/wave-data?spotterId=SPOT-31071C&token=REPALCE_TOKEN_STRING&includeWaves=true&includeTrack=true&includeDirectionalMoments=true&limit=500	1	0	\N		100	0	line
152	SPOT-1905	SPOT-1905	-13.64255	-172.04037	SamoaMet	\N	f	significantWaveHeight,peakPeriod,peakDirection,timestamp	significant_wave_height,peak_wave_period,peak_wave_direction,time	\N	1	1	1	12	https://wavefleet.spoondriftspotter.co/api/wave-data?spotterId=SPOT-1905&token=REPALCE_TOKEN_STRING&includeWaves=true&includeTrack=true&includeDirectionalMoments=true&limit=500	4	0	\N	Samoa, Wavebuoy	100	0	line
153	SPOT-1611	SPOT-1611	-13.81497	-171.78047	SamoaMet	\N	f	significantWaveHeight,peakPeriod,peakDirection,timestamp	significant_wave_height,peak_wave_period,peak_wave_direction,time	\N	1	1	1	12	https://wavefleet.spoondriftspotter.co/api/wave-data?spotterId=SPOT-1611&token=REPALCE_TOKEN_STRING&includeWaves=true&includeTrack=true&includeDirectionalMoments=true&limit=500	4	0	\N	Samoa, Wavebuoy	100	0	line
132	SPOT-30815C	SPOT-30815C	-18.19448	178.35897	SPC	\N	f	significantWaveHeight,peakPeriod,peakDirection,timestamp	significant_wave_height,peak_wave_period,peak_wave_direction,time	\N	1	1	1	2	https://wavefleet.spoondriftspotter.co/api/wave-data?spotterId=SPOT-30815C&token=REPALCE_TOKEN_STRING&includeWaves=true&includeTrack=true&includeDirectionalMoments=true&limit=500	1	0	\N	Fiji, Suva Wavebuoy	100	0	line
134	SPOT-0299	SPOT-0299	-18.22757	177.77305	FMS	\N	f	significantWaveHeight,peakPeriod,peakDirection,timestamp	significant_wave_height,peak_wave_period,peak_wave_direction,time	\N	1	1	1	2	https://wavefleet.spoondriftspotter.co/api/wave-data?spotterId=SPOT-0299&token=REPALCE_TOKEN_STRING&includeWaves=true&includeTrack=true&includeDirectionalMoments=true&limit=500	2	0	\N	Fiji, Komave, Wavebuoy	100	0	line
155	SPOT-1901	SPOT-1901	-26.78888	-169.9395	TongaMet	\N	f	significantWaveHeight,peakPeriod,peakDirection,timestamp	significant_wave_height,peak_wave_period,peak_wave_direction,time	\N	1	1	2	3	https://wavefleet.spoondriftspotter.co/api/wave-data?spotterId=SPOT-1901&token=REPALCE_TOKEN_STRING&includeWaves=true&includeTrack=true&includeDirectionalMoments=true&limit=500	5	0	\N	\N	100	0	line
139	SPOT-0300	SPOT-0300	1.42755	172.91135	KMS	\N	f	significantWaveHeight,peakPeriod,peakDirection,timestamp	significant_wave_height,peak_wave_period,peak_wave_direction,time	\N	1	1	1	6	https://wavefleet.spoondriftspotter.co/api/wave-data?spotterId=SPOT-0300&token=REPALCE_TOKEN_STRING&includeWaves=true&includeTrack=true&includeDirectionalMoments=true&limit=500	3	0	\N	Kiribati, Tarawa Wavebuoy	100	0	line
140	SPOT-1930	SPOT-1930	1.36343	173.08808	KMS	\N	f	significantWaveHeight,peakPeriod,peakDirection,timestamp	significant_wave_height,peak_wave_period,peak_wave_direction,time	\N	1	1	1	6	https://wavefleet.spoondriftspotter.co/api/wave-data?spotterId=SPOT-1930&token=REPALCE_TOKEN_STRING&includeWaves=true&includeTrack=true&includeDirectionalMoments=true&limit=500	3	0	\N	Kiribati, Tarawa, Wavebuoy	100	0	line
130	SPOT-1931	SPOT-1931	-20.50715	165.09363	SPC	\N	f	significantWaveHeight,peakPeriod,peakDirection,timestamp	significant_wave_height,peak_wave_period,peak_wave_direction,time	\N	1	1	1	20	https://wavefleet.spoondriftspotter.co/api/wave-data?spotterId=SPOT-1931&token=REPALCE_TOKEN_STRING&includeWaves=true&includeTrack=true&includeDirectionalMoments=true&limit=500	1	0	\N	Noumea, Wavebuoy	100	0	line
171	SPOT-0433	SPOT-0433	-22.57357	166.45005	SPC	\N	f	significantWaveHeight,peakPeriod,peakDirection,timestamp	significant_wave_height,peak_wave_period,peak_wave_direction,time	\N	1	1	1	20	https://wavefleet.spoondriftspotter.co/api/wave-data?spotterId=SPOT-0433&token=REPALCE_TOKEN_STRING&includeWaves=true&includeTrack=true&includeDirectionalMoments=true&limit=500	1	0	\N	Noumea, Wavebuoy	100	0	line
131	SPOT-30187R	SPOT-30187R	-21.74685	165.33293	SPC	\N	f	significantWaveHeight,peakPeriod,peakDirection,timestamp	significant_wave_height,peak_wave_period,peak_wave_direction,time	\N	1	1	1	20	https://wavefleet.spoondriftspotter.co/api/wave-data?spotterId=SPOT-30187R&token=REPALCE_TOKEN_STRING&includeWaves=true&includeTrack=true&includeDirectionalMoments=true&limit=500	1	0	\N	Noumea, Wavebuoy	100	0	line
126	SPOT-0298	SPOT-0298	-19.30632	177.96413	SPC	\N	f	significantWaveHeight,peakPeriod,peakDirection,timestamp	significant_wave_height,peak_wave_period,peak_wave_direction,time	\N	1	1	1	2	https://wavefleet.spoondriftspotter.co/api/wave-data?spotterId=SPOT-0298&token=REPALCE_TOKEN_STRING&includeWaves=true&includeTrack=true&includeDirectionalMoments=true&limit=500	1	0	\N	Fiji, Kadavu, Wavebuoy	100	0	line
170	SPOT-0285	SPOT-0285	-19.1278	177.95632	SPC	\N	f	significantWaveHeight,peakPeriod,peakDirection,timestamp	significant_wave_height,peak_wave_period,peak_wave_direction,time	\N	1	1	2	2	https://wavefleet.spoondriftspotter.co/api/wave-data?spotterId=SPOT-0285&token=REPALCE_TOKEN_STRING&includeWaves=true&includeTrack=true&includeDirectionalMoments=true&limit=500	1	0	\N	\N	100	0	line
125	SPOT-0285	SPOT-0285	-19.1278	177.95632	SPC	\N	f	significantWaveHeight,peakPeriod,peakDirection,timestamp	significant_wave_height,peak_wave_period,peak_wave_direction,time	\N	1	1	2	2	https://wavefleet.spoondriftspotter.co/api/wave-data?spotterId=SPOT-0285&token=REPALCE_TOKEN_STRING&includeWaves=true&includeTrack=true&includeDirectionalMoments=true&limit=500	1	0	\N	\N	100	0	line
167	SPOT-31071C	SPOT-31071C	-19.0662333	-169.98535	NMS	\N	f	significantWaveHeight,peakPeriod,peakDirection,timestamp	significant_wave_height,peak_wave_period,peak_wave_direction,time	\N	1	1	2	9	https://wavefleet.spoondriftspotter.co/api/wave-data?spotterId=SPOT-31071C&token=REPALCE_TOKEN_STRING&includeWaves=true&includeTrack=true&includeDirectionalMoments=true&limit=500	7	0	\N	\N	100	0	line
142	SPOT-1906	SPOT-1906	-13.81498	-171.78053	SamoaMet	\N	f	significantWaveHeight,peakPeriod,peakDirection,timestamp	significant_wave_height,peak_wave_period,peak_wave_direction,time	\N	1	1	1	12	https://wavefleet.spoondriftspotter.co/api/wave-data?spotterId=SPOT-1906&token=REPALCE_TOKEN_STRING&includeWaves=true&includeTrack=true&includeDirectionalMoments=true&limit=500	4	0	\N	Samoa, Wavebuoy	100	0	line
127	SPOT-0089	SPOT-0089	-18.17648	178.41657	SPC	\N	f	significantWaveHeight,peakPeriod,peakDirection,timestamp	significant_wave_height,peak_wave_period,peak_wave_direction,time	\N	1	1	1	2	https://wavefleet.spoondriftspotter.co/api/wave-data?spotterId=SPOT-0089&token=REPALCE_TOKEN_STRING&includeWaves=true&includeTrack=true&includeDirectionalMoments=true&limit=500	1	0	\N	Fiji, Suva, Wavebuoy	100	0	line
141	SPOT-1943	SPOT-1943	-5.49172	159.69458	KMS	\N	f	significantWaveHeight,peakPeriod,peakDirection,timestamp	significant_wave_height,peak_wave_period,peak_wave_direction,time	\N	1	1	2	6	https://wavefleet.spoondriftspotter.co/api/wave-data?spotterId=SPOT-1943&token=REPALCE_TOKEN_STRING&includeWaves=true&includeTrack=true&includeDirectionalMoments=true&limit=500	3	0	\N	\N	100	0	line
174	 120 NM NW of Valparaiso        	32404	-32.13	-73.8	Cooperative Effort DART 4G	\N	t	time,m	time,sea_level (m)	\N	3	3	2	0	https://www.ndbc.noaa.gov/dart_data.php?station=STATION_ID&startmonth=START_MONTH&startday=START_DAY&startyear=START_YEAR&endmonth=END_MONTH&endday=END_DAY&endyear=END_YEAR	\N	0	\N	 120 NM NW of Valparaiso        	100	0	line
188	 NORTHWEST APIA - 370 NM NW of Apia, Samoa 	51425	-9.51	-176.26	NDBC	\N	t	time,m	time,sea_level (m)	\N	3	3	1	12	https://www.ndbc.noaa.gov/dart_data.php?station=STATION_ID&startmonth=START_MONTH&startday=START_DAY&startyear=START_YEAR&endmonth=END_MONTH&endday=END_DAY&endyear=END_YEAR	\N	0	\N	 NORTHWEST APIA - 370 NM NW of Apia, Samoa 	100	-4951.48	line
177	 NORTH SANTO DOMINGO - 328NM NNE of Santo Domingo, DO 	41420	23.43	-67.39	NDBC	\N	t	time,m	time,sea_level (m)	\N	3	3	2	0	https://www.ndbc.noaa.gov/dart_data.php?station=STATION_ID&startmonth=START_MONTH&startday=START_DAY&startyear=START_YEAR&endmonth=END_MONTH&endday=END_DAY&endyear=END_YEAR	\N	0	\N	 NORTH SANTO DOMINGO - 328NM NNE of Santo Domingo, DO 	100	0	line
180	 SOUTH PUERTO RICO - 230 NM Southwest of San Juan, PR 	42407	15.28	-68.19	NDBC	\N	t	time,m	time,sea_level (m)	\N	3	3	1	0	https://www.ndbc.noaa.gov/dart_data.php?station=STATION_ID&startmonth=START_MONTH&startday=START_DAY&startyear=START_YEAR&endmonth=END_MONTH&endday=END_DAY&endyear=END_YEAR	\N	0	\N	 SOUTH PUERTO RICO - 230 NM Southwest of San Juan, PR 	100	-4498.81	line
203	 Coral Sea 2     -     870km NE of Townsville 	55023	-14.71	153.54	Australian Bureau of Meteorology	\N	t	time,m	time,sea_level (m)	\N	3	3	1	0	https://www.ndbc.noaa.gov/dart_data.php?station=STATION_ID&startmonth=START_MONTH&startday=START_DAY&startyear=START_YEAR&endmonth=END_MONTH&endday=END_DAY&endyear=END_YEAR	\N	0	\N	 Coral Sea 2     -     870km NE of Townsville 	100	-4592.27	line
190	 SOUTH PHILIPPINE SEA -725 NM West of Agana, Guam 	52405	12.99	132.23	NDBC	\N	t	time,m	time,sea_level (m)	\N	3	3	1	24	https://www.ndbc.noaa.gov/dart_data.php?station=STATION_ID&startmonth=START_MONTH&startday=START_DAY&startyear=START_YEAR&endmonth=END_MONTH&endday=END_DAY&endyear=END_YEAR	\N	0	\N	 SOUTH PHILIPPINE SEA -725 NM West of Agana, Guam 	100	-5796.61	line
194	 NZG - Offshore Tongatapu Tonga 	5401003	-23.35	-173.4	New Zealand National Emergency Management Agency by Te Pu Ao GNS Science and NIWA Taihoro Nukurangi	\N	t	time,m	time,sea_level (m)	\N	3	3	1	3	https://www.ndbc.noaa.gov/dart_data.php?station=STATION_ID&startmonth=START_MONTH&startday=START_DAY&startyear=START_YEAR&endmonth=END_MONTH&endday=END_DAY&endyear=END_YEAR	\N	0	\N	 NZG - Offshore Tongatapu Tonga 	100	-5738.63	line
199	 NZJ - Offshore Norfolk Island New Hebrides 	5501005	-26.67	163.96	New Zealand National Emergency Management Agency by Te Pu Ao GNS Science and NIWA Taihoro Nukurangi	\N	t	time,m	time,sea_level (m)	\N	3	3	1	0	https://www.ndbc.noaa.gov/dart_data.php?station=STATION_ID&startmonth=START_MONTH&startday=START_DAY&startyear=START_YEAR&endmonth=END_MONTH&endday=END_DAY&endyear=END_YEAR	\N	0	\N	 NZJ - Offshore Norfolk Island New Hebrides 	100	-1912.16	line
200	 NZK - Offshore New Caledonia New Hebrides 	5501006	-24.31	169.5	New Zealand National Emergency Management Agency by Te Pu Ao GNS Science and NIWA Taihoro Nukurangi	\N	t	time,m	time,sea_level (m)	\N	3	3	1	0	https://www.ndbc.noaa.gov/dart_data.php?station=STATION_ID&startmonth=START_MONTH&startday=START_DAY&startyear=START_YEAR&endmonth=END_MONTH&endday=END_DAY&endyear=END_YEAR	\N	0	\N	 NZK - Offshore New Caledonia New Hebrides 	100	-2097.86	line
204	 Indian Ocean 2     -     630km NNE of Dampier 	56003	-15.02	118.07	Australian Bureau of Meteorology	\N	t	time,m	time,sea_level (m)	\N	3	3	1	0	https://www.ndbc.noaa.gov/dart_data.php?station=STATION_ID&startmonth=START_MONTH&startday=START_DAY&startyear=START_YEAR&endmonth=END_MONTH&endday=END_DAY&endyear=END_YEAR	\N	0	\N	 Indian Ocean 2     -     630km NNE of Dampier 	100	-5658.5	line
189	 SOUTHEAST SAIPAN - 540NM ESE of Saipan 	52402	11.93	153.88	NDBC	\N	t	time,m	time,sea_level (m)	\N	3	3	1	0	https://www.ndbc.noaa.gov/dart_data.php?station=STATION_ID&startmonth=START_MONTH&startday=START_DAY&startyear=START_YEAR&endmonth=END_MONTH&endday=END_DAY&endyear=END_YEAR	\N	0	\N	 SOUTHEAST SAIPAN - 540NM ESE of Saipan 	100	-5886.35	line
193	 NZF - Offshore Raoul Island Kermadec 	5401002	-29.68	-175.01	New Zealand National Emergency Management Agency by Te Pu Ao GNS Science and NIWA Taihoro Nukurangi	\N	t	time,m	time,sea_level (m)	\N	3	3	1	0	https://www.ndbc.noaa.gov/dart_data.php?station=STATION_ID&startmonth=START_MONTH&startday=START_DAY&startyear=START_YEAR&endmonth=END_MONTH&endday=END_DAY&endyear=END_YEAR	\N	0	\N	 NZF - Offshore Raoul Island Kermadec 	100	-5063.99	line
202	 Coral Sea 1     -     1285km ENE of Townsville 	55012	-15.66	158.45	Australian Bureau of Meteorology	\N	t	time,m	time,sea_level (m)	\N	3	3	1	0	https://www.ndbc.noaa.gov/dart_data.php?station=STATION_ID&startmonth=START_MONTH&startday=START_DAY&startyear=START_YEAR&endmonth=END_MONTH&endday=END_DAY&endyear=END_YEAR	\N	0	\N	 Coral Sea 1 - 1285km ENE of Townsville 	100	-3346.89	line
172	 KURIL ISLANDS - 209NM SE of Kuril Is.  	21419	44.4	155.65	 NDBC           	\N	t	time,m	time,sea_level (m)	\N	3	3	1	0	https://www.ndbc.noaa.gov/dart_data.php?station=STATION_ID&startmonth=START_MONTH&startday=START_DAY&startyear=START_YEAR&endmonth=END_MONTH&endday=END_DAY&endyear=END_YEAR	\N	0	\N	 KURIL ISLANDS - 209NM SE of Kuril Is.  	100	-5321.99	line
173	 180 NM  West of Caldera, Chile 	32402	-26.74	-73.98	Hydrographic and Oceanographic Service of the Chilean Navy (SHOA)	\N	t	time,m	time,sea_level (m)	\N	3	3	1	0	https://www.ndbc.noaa.gov/dart_data.php?station=STATION_ID&startmonth=START_MONTH&startday=START_DAY&startyear=START_YEAR&endmonth=END_MONTH&endday=END_DAY&endyear=END_YEAR	\N	0	\N	 180 NM  West of Caldera, Chile 	100	-4059.81	line
175	 NORTHWEST LIMA - 1000 NM WNW of Lima, Peru 	32413	-7.43	-93.46	NDBC	\N	t	time,m	time,sea_level (m)	\N	3	3	1	0	https://www.ndbc.noaa.gov/dart_data.php?station=STATION_ID&startmonth=START_MONTH&startday=START_DAY&startyear=START_YEAR&endmonth=END_MONTH&endday=END_DAY&endyear=END_YEAR	\N	0	\N	 NORTHWEST LIMA - 1000 NM WNW of Lima, Peru 	100	-3970.21	line
176	 119NM NW of Concepcion, Chile  	34420	-35.76	-75.24	Cooperative Effort DART 4G	\N	t	time,m	time,sea_level (m)	\N	3	3	1	0	https://www.ndbc.noaa.gov/dart_data.php?station=STATION_ID&startmonth=START_MONTH&startday=START_DAY&startyear=START_YEAR&endmonth=END_MONTH&endday=END_DAY&endyear=END_YEAR	\N	0	\N	 119NM NW of Concepcion, Chile  	100	-4125.65	line
357	Haapai Sea Level	11761	-19.8022513219537	-174.352260751473	SPC		t	time,sea_level_265149	time,sea_level		4	5	1	1	https://restservice-neon.niwa.co.nz/NeonRESTService.svc/GetDataResampled/265149?StartTime=START_TIME&EndTime=END_TIMEInterval=1&Method=mean|https://restservice-neon.niwa.co.nz/NeonRESTService.svc/GetDataResampled/265150?StartTime=START_TIME&EndTime=END_TIMEInterval=1&Method=mean	10	0		Tonga - Haapai Sea Level	100	0	line
359	Niuatoputapu Sea Level	7614	-15.9422669	-173.7665069	SPC		t	time,sea_level_159544	time,sea_level		4	5	1	1	https://restservice-neon.niwa.co.nz/NeonRESTService.svc/GetDataResampled/159544?StartTime=START_TIME&EndTime=END_TIMEInterval=1&Method=mean|https://restservice-neon.niwa.co.nz/NeonRESTService.svc/GetDataResampled/159545?StartTime=START_TIME&EndTime=END_TIMEInterval=1&Method=mean	10	0		Tonga - Niuatoputapu Sea Level	100	0	line
356	Eua Sea Level	12528	-21.3381943396256	-174.955846638259	SPC		t	time,sea_level_273416	time,sea_level		4	5	1	1	https://restservice-neon.niwa.co.nz/NeonRESTService.svc/GetDataResampled/273416?StartTime=START_TIME&EndTime=END_TIMEInterval=1&Method=mean|https://restservice-neon.niwa.co.nz/NeonRESTService.svc/GetDataResampled/273417?StartTime=START_TIME&EndTime=END_TIMEInterval=1&Method=mean	10	0		Tonga - Eua Sea Level	100	0	line
358	Neiafu Sea Level	7869	-18.652501	-173.9844864	SPC		t	time,sea_level_167559	time,sea_level		4	5	1	1	https://restservice-neon.niwa.co.nz/NeonRESTService.svc/GetDataResampled/167559?StartTime=START_TIME&EndTime=END_TIMEInterval=1&Method=mean|https://restservice-neon.niwa.co.nz/NeonRESTService.svc/GetDataResampled/167560?StartTime=START_TIME&EndTime=END_TIMEInterval=1&Method=mean	10	0	\N	Tonga - Neiafu Sea Level	100	0	line
7	AMCHITKA - 170 NM South of Amchitka, AK	21414	13.3555	144.788	NDBC	NDBC	t	time,m	time,sea_level (m)	instrument	3	3	1	24	https://www.ndbc.noaa.gov/dart_data.php?station=STATION_ID&startmonth=START_MONTH&startday=START_DAY&startyear=START_YEAR&endmonth=END_MONTH&endday=END_DAY&endyear=END_YEAR	\N	0	\N	AMCHITKA - 170 NM South of Amchitka, AK	100	-5442.72	line
192	 NZE - Offshore East Cape Kermadec 	5401001	-36.05	-177.71	New Zealand National Emergency Management Agency by Te Pu Ao GNS Science and NIWA Taihoro Nukurangi	\N	t	time,m	time,sea_level (m)	\N	3	3	1	0	https://www.ndbc.noaa.gov/dart_data.php?station=STATION_ID&startmonth=START_MONTH&startday=START_DAY&startyear=START_YEAR&endmonth=END_MONTH&endday=END_DAY&endyear=END_YEAR	\N	0	\N	 NZE - Offshore East Cape Kermadec 	100	-5777.27	line
165	276	276	5.24042	163.0008	PACIOOS	\N	t	time,waveHs,waveTp,waveDp	time,significant_wave_height,peak_wave_period,peak_wave_direction	\N	1	2	1	5	https://erddap.cdip.ucsd.edu/erddap/tabledap/wave_agg.geoJson?station_id,time,waveHs,waveTp,waveTa,waveDp,latitude,longitude&station_id=%22STATION_ID%22&time%3E=START_TIME&time%3C=END_TIME&waveFlagPrimary=1	\N	0	\N	FSM, Wavebuoy	100	0	line
244	dnzc	dnzc	-38.2001	-179.7978	GNS Science ( New Zealand )	GNS Science ( New Zealand )	t	slevel,stime	sea_level (m),time	\N	3	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	New Zealand, E Coast - DART Buoy	100	-3545.43	line
360	Nomuka Sea Level	12537	-20.25018173189	-174.81551775139	SPC		t	time,sea_level_273802	time,sea_level		4	5	1	1	https://restservice-neon.niwa.co.nz/NeonRESTService.svc/GetDataResampled/273802?StartTime=START_TIME&EndTime=END_TIMEInterval=1&Method=mean|https://restservice-neon.niwa.co.nz/NeonRESTService.svc/GetDataResampled/273803?StartTime=START_TIME&EndTime=END_TIMEInterval=1&Method=mean	10	0		Tonga - Nomuka Sea Level	100	0	line
312	pagx	pagx	-14.2766	-170.6907	National Ocean Service-NOAA ( USA )	National Ocean Service-NOAA ( USA )	t	slevel,stime	sea_level (m),time	\N	4	4	2	18	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Samoa, Pago Pago AS - Tide Gauge	100	0	line
211	aunuu	aunuu	-14.283449	-170.561021	University of Hawaii Sea Level Center ( USA )	University of Hawaii Sea Level Center ( USA )	t	slevel,stime	sea_level (m),time	\N	4	4	1	18	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&includesensors%5B%5D=rad&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	<0.2	Samoa, Aunuu - Tide Gauge	1000	0	scatter
209	auasi	auasi	-14.271544	-170.57307	University of Hawaii Sea Level Center ( USA )	University of Hawaii Sea Level Center ( USA )	t	slevel,stime	sea_level (m),time	\N	4	4	1	18	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&includesensors%5B%5D=rad&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Samoa, Auasi - Tide Gauge	1000	0	scatter
229	dcor2	dcor2	-15.7109	158.4644	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	3	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Australia, Townsville - DART Buoy	100	-3346.97	line
230	dcr2	dcr2	-14.7148	153.5374	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	3	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Australia, Townsville - DART Buoy	100	-4592.66	line
256	dnzi	dnzi	-16.889	-171.1905	GNS Science ( New Zealand )	GNS Science ( New Zealand )	t	slevel,stime	sea_level (m),time	\N	3	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	New Zealand, Tonga - DART Buoy	100	-5236.46	line
257	dnzi2	dnzi2	-16.889	-171.1905	GNS Science ( New Zealand )	GNS Science ( New Zealand )	t	slevel,stime	sea_level (m),time	\N	3	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	New Zealand, Tonga - DART Buoy	100	-5236.69	line
258	dnzj	dnzj	-26.6672	163.9549	GNS Science ( New Zealand )	GNS Science ( New Zealand )	t	slevel,stime	sea_level (m),time	\N	3	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	New Zealand - DART Buoy	100	-1912.29	line
259	dnzj2	dnzj2	-26.6672	163.9549	GNS Science ( New Zealand )	GNS Science ( New Zealand )	t	slevel,stime	sea_level (m),time	\N	3	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	New Zealand - DART Buoy	100	-1912.29	line
266	dta4	dta4	-44.8978	161.8412	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	3	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Australia, Hobart - DART Buoy	100	-4828.99	line
267	dtas	dtas	-46.8295	160.2562	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	3	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Australia, Hobart - DART Buoy	100	-4991.87	line
269	dtru	dtru	4.0183	145.54	National Ocean Service-NOAA ( USA )	National Ocean Service-NOAA ( USA )	t	slevel,stime	sea_level (m),time	\N	3	4	1	11	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	PNG, Manus Is - DART Buoy	100	-4434.22	line
231	dcr3	dcr3	-14.7148	153.5374	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	3	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Australia, Townsville - DART Buoy	100	-4592.29	line
234	dgum	dgum	12.985	132.165	National Ocean Service-NOAA ( USA )	National Ocean Service-NOAA ( USA )	t	slevel,stime	sea_level (m),time	\N	3	4	1	24	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Guam,  Agana - DART Buoy	100	-5796.36	line
236	djve	djve	-15.0234	117.9418	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	3	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Australia, Dampier - DART Buoy	100	-5658.8	line
237	djve2	djve2	-15.0234	117.9418	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	3	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Australia, Dampier - DART Buoy	100	-5658.8	line
212	bapj	bapj	-42.8923	147.338	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	4	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Australia, Battery Point - Tide Gauge	1000	0	line
240	dnza	dnza	-42.3707	176.9109	GNS Science ( New Zealand )	GNS Science ( New Zealand )	t	slevel,stime	sea_level (m),time	\N	3	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	New Zealand, E Coast - DART Buoy	100	-2634.7	line
213	barn	barn	-41.0501	145.915	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	4	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Australia, Burnie, Tasmania - Tide Gauge	1000	0	line
241	dnza2	dnza2	-42.3707	176.9109	GNS Science ( New Zealand )	GNS Science ( New Zealand )	t	slevel,stime	sea_level (m),time	\N	3	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	New Zealand, E Coast - DART Buoy	100	-2634.7	line
255	dnzh2	dnzh2	-20.0885	-171.863	GNS Science ( New Zealand )	GNS Science ( New Zealand )	t	slevel,stime	sea_level (m),time	\N	3	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	New Zealand, Tonga - DART Buoy	100	-5530.57	line
214	brom	brom	-18.0008	122.2186	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	4	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Australia, Broome AU - Tide Gauge	1000	0	line
219	chit	chit	-44.0247	-176.3688	Land Information New Zealand ( New Zealand )	Land Information New Zealand ( New Zealand )	t	slevel,stime	sea_level (m),time	\N	4	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	New Zealand, Owenga, Chatham Island - Tide Gauge	1000	0	line
220	chrs	chrs	-10.4294	105.6693	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	4	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Australia, Chrismatas Island AU - Tide Gauge	1000	0	line
207	abas	abas	44.02	144.29	Japan Meteorological Agency	Japan Meteorological Agency	t	slevel,stime	sea_level (m),time		4	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Japan,  Abashiri Tide Gauge	1000	0	line
221	chst	chst	-41.9032	171.4338	Land Information New Zealand ( New Zealand )	Land Information New Zealand ( New Zealand )	t	slevel,stime	sea_level (m),time	\N	4	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	New Zealand, Charleston - Tide Gauge	1000	0	line
223	cocb	cocb	-12.1167	96.8919	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	4	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Australia, Cocos Island AU - Tide Gauge	1000	0	line
224	cpit	cpit	-40.89929	176.2316763	Land Information New Zealand ( New Zealand )	Land Information New Zealand ( New Zealand )	t	slevel,stime	sea_level (m),time	\N	4	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	New Zealand, Castlepoint - Tide Gauge	1000	0	line
242	dnzb	dnzb	-40.5992	179.0962	GNS Science ( New Zealand )	GNS Science ( New Zealand )	t	slevel,stime	sea_level (m),time	\N	3	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	New Zealand, E Coast - DART Buoy	100	-3184.37	line
226	dapi	dapi	-9.5183	-176.2469	National Ocean Service-NOAA ( USA )	National Ocean Service-NOAA ( USA )	t	slevel,stime	sea_level (m),time	\N	3	4	1	18	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Samoa, Apia - DART Buoy	100	-4950.8	line
252	dnzg	dnzg	-23.3517	-173.4018	GNS Science ( New Zealand )	GNS Science ( New Zealand )	t	slevel,stime	sea_level (m),time	\N	3	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	New Zealand, Tonga - DART Buoy	100	-5739.23	line
253	dnzg2	dnzg2	-23.3517	-173.4018	GNS Science ( New Zealand )	GNS Science ( New Zealand )	t	slevel,stime	sea_level (m),time	\N	3	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	New Zealand, Tonga - DART Buoy	100	-5739.23	line
254	dnzh	dnzh	-20.0885	-171.863	GNS Science ( New Zealand )	GNS Science ( New Zealand )	t	slevel,stime	sea_level (m),time	\N	3	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	New Zealand, Tonga - DART Buoy	100	-5531.07	line
268	dtas2	dtas2	-46.8295	160.2562	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	3	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Australia, Hobart - DART Buoy	100	-4991.15	line
246	dnzd	dnzd	-36.0999	178.6037	GNS Science ( New Zealand )	GNS Science ( New Zealand )	t	slevel,stime	sea_level (m),time	\N	3	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	New Zealand - DART Buoy	100	-2447.63	line
250	dnzf	dnzf	-29.6826	-175.0125	GNS Science ( New Zealand )	GNS Science ( New Zealand )	t	slevel,stime	sea_level (m),time	\N	3	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	New Zealand, Tonga - DART Buoy	100	-5064.68	line
260	dnzk	dnzk	-24.3093	169.4988	GNS Science ( New Zealand )	GNS Science ( New Zealand )	t	slevel,stime	sea_level (m),time	\N	3	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	New Zealand - DART Buoy	100	-2097.82	line
261	dnzk2	dnzk2	-24.3093	169.4988	GNS Science ( New Zealand )	GNS Science ( New Zealand )	t	slevel,stime	sea_level (m),time	\N	3	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	New Zealand - DART Buoy	100	-2097.82	line
225	cuvie	cuvie	-24.2206	113.3969	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	4	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Australia, Cape Cuvier Wharf - Tide Gauge	1000	0	line
294	lott	lott	-37.5503	178.159	Land Information New Zealand ( New Zealand )	Land Information New Zealand ( New Zealand )	t	slevel,stime	sea_level (m),time	\N	4	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	New Zealand, East Cape - Tide Gauge	1000	0	line
227	darw	darw	-12.4719	130.8458	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	4	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Australia, Darwin AU - Tide Gauge	1000	0	line
272	ffcj	ffcj	-10.4294	105.6694	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	4	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Australia, Flying Fish Cove Jetty, Chrismat Is AU - Tide Gauge	1000	0	line
273	fong	fong	-8.5033	179.1949	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	4	4	1	4	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	-999	Tuvalu, Fongafale TV - Tide Gauge	1000	0	line
274	gbit	gbit	-36.189	175.4889	Land Information New Zealand ( New Zealand )	Land Information New Zealand ( New Zealand )	t	slevel,stime	sea_level (m),time	\N	4	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	New Zealand, Great Barrier Is - Tide Gauge	1000	0	line
275	gcsb	gcsb	-27.9387	153.4326	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	4	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Australia, Gold Coast Sand Bypass Jetty - Tide Gauge	1000	0	line
277	gist	gist	-38.6754097	178.0228774	Land Information New Zealand ( New Zealand )	Land Information New Zealand ( New Zealand )	t	slevel,stime	sea_level (m),time	\N	4	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	New Zealand, Gisborne	1000	0	line
338	thev	thev	-32.1489	133.6413	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	4	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Australia, Thevenard AU - Tide Gauge	1000	0	line
280	hill	hill	-31.8255	115.7386	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	4	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Australia, Hillarys Harbour AU - Tide Gauge	1000	0	line
281	jack	jack	-43.9733	168.6161	Land Information New Zealand ( New Zealand )	Land Information New Zealand ( New Zealand )	t	slevel,stime	sea_level (m),time	\N	4	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	New Zealand, Jackson Bay - Tide Gauge	1000	0	line
282	kait	kait	-42.4129	173.7028	Land Information New Zealand ( New Zealand )	Land Information New Zealand ( New Zealand )	t	slevel,stime	sea_level (m),time	\N	4	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	New Zealand, Kaikoura - Tide Gauge 	1000	0	line
285	kjni	kjni	-29.0591	167.9536	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	4	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Norfolk Is, Kingston Jetty - Tide Gauge	1000	0	line
283	kant	kant	-2.801	-171.718	Kiribati Met Office ( Kiribati )	Kiribati Met Office ( Kiribati )	t	slevel,stime	sea_level (m),time	\N	4	4	1	6	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&includesensors%5B%5D=rad&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Kiribati, Kanton KI - Tide Gauge 	1000	0	line
233	dfij	dfij	-26	176	NULL	NULL	t	slevel,stime	sea_level (m),time	\N	3	4	2	2	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Fiji, Fiji Basin - DART Buoy	100	0	line
232	deke	deke	6.9806	158.2001	National Weather Service ( Micronesia )	National Weather Service ( Micronesia )	t	slevel,stime	sea_level (m),time	\N	4	4	1	5	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Mirconesia, Dekehtik, Pohnpei FM - Tide Gauge	1000	0	line
306	nkfa	nkfa	-21.1303	-175.1967	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	4	4	1	3	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Tonga, Nuku'Alofa Harbour Wharf - Tide Gauge	1000	0	line
270	espe	espe	-33.8709	121.8954	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	4	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Australia, Esperance AU - Tide Gauge	1000	0	line
286	kwaj	kwaj	8.7367	167.7383	National Ocean Service-NOAA ( USA )	National Ocean Service-NOAA ( USA )	t	slevel,stime	sea_level (m),time	\N	4	4	1	7	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Marshall Is, Kwajalein - Tide Gauge	1000	0	line
289	lena	lena	-19.53256	169.265953	Mineral Resources Department ( Vanuatu )	Mineral Resources Department ( Vanuatu )	t	slevel,stime	sea_level (m),time	\N	4	4	1	14	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Vanuatu, Lenakel, Tanna - Tide Gauge	1000	0	line
290	levu	levu	-17.6049	177.4383	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	4	4	1	2	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	-999	Fiji, Lautoka FJ - Tide Gauge	1000	0	line
291	lirf	lirf	-17.133	152.145	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	4	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Australia, Lihou Reef AU - Tide Gauge	1000	0	line
292	litz	litz	-16.11283	167.44397	NULL	NULL	t	slevel,stime	sea_level (m),time	\N	4	4	1	14	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Vanuatu, Litzlitz, Malekula - Tide Gauge 	1000	0	line
307	nkfa2	nkfa2	-21.1303	-175.1967	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	4	4	2	3	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Tonga, Nuku'Alofa Harbour Wharf - Tide Gauge	100	0	line
293	lomb	lomb	-2.0421	147.3737	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	4	4	1	11	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	PNG, Lombrum, Manus Is PG - Tide Gauge	1000	0	line
295	luga	luga	-15.5156	167.1886	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	4	4	1	14	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Vanuatu, Luganville - Tide Gauge	1000	0	line
297	marsh	marsh	7.1061	171.3725	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	4	4	1	7	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Marshall Is, Majuro - Tide Gauge	1000	0	line
298	mnkt	mnkt	-37.0466	174.5117	Land Information New Zealand ( New Zealand )	Land Information New Zealand ( New Zealand )	t	slevel,stime	sea_level (m),time	\N	4	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	New Zealand, Manukau - Tide Gauge 	1000	0	line
296	mala	mala	7.3282	134.4502	University of Hawaii Sea Level Center ( USA )	University of Hawaii Sea Level Center ( USA )	t	slevel,stime	sea_level (m),time	\N	4	4	2	10	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&includesensors%5B%5D=rad&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Palau, Malakal - Tide Gauge	100	0	line
300	napt	napt	-39.4756612	176.920066	Land Information New Zealand ( New Zealand )	Land Information New Zealand ( New Zealand )	t	slevel,stime	sea_level (m),time	\N	4	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	New Zealand, Port Napier - Tide Gauge	1000	0	line
302	nauu	nauu	-0.5319	166.9092	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	4	4	1	8	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Nauru - Tide Gauge	1000	0	line
303	nbrt	nbrt	-43.506	172.735	Land Information New Zealand ( New Zealand )	Land Information New Zealand ( New Zealand )	t	slevel,stime	sea_level (m),time	\N	4	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	New Zealand, New Brighton, Christchurch - Tide Gauge	1000	0	line
304	ncpt	ncpt	-34.41	173.05	Land Information New Zealand ( New Zealand )	Land Information New Zealand ( New Zealand )	t	slevel,stime	sea_level (m),time	\N	4	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	New Zealand, North Cape - Tide Gauge 	1000	0	line
309	otat	otat	-45.8143	170.6294	Land Information New Zealand ( New Zealand )	Land Information New Zealand ( New Zealand )	t	slevel,stime	sea_level (m),time	\N	4	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	New Zealand, Dunedin - Tide Gauge	1000	0	line
315	pkem	pkem	-34.4738	150.9119	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	4	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Austalia, Port Kembla AU - Tide Gauge	1000	0	line
316	pmur	pmur	-21.8167	114.191	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	4	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Australia, Port Kembla AU - Tide Gauge	1000	0	line
323	raro	raro	-21.2	-159.783	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	4	4	1	16	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Cook Is, Rarotonga CK - Tide Gauge	1000	0	line
326	rfrt	rfrt	-29.251144	-177.903842	Land Information New Zealand ( New Zealand )	Land Information New Zealand ( New Zealand )	t	slevel,stime	sea_level (m),time	\N	4	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	New Zealand, Raoul Island Fishing Rock - Tide Gauge 	1000	0	line
328	ross	ross	-23.161	150.7902	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	4	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Australia, Rosslyn Bay AU - Tide Gauge 	1000	0	line
329	solo	solo	-9.4289	159.9555	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	4	4	1	13	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Solomon Is, Honiara - Tide Gauge	1000	0	line
310	pago	pago	-14.2766	-170.6907	National Ocean Service-NOAA ( USA )	National Ocean Service-NOAA ( USA )	t	slevel,stime	sea_level (m),time	\N	4	4	1	18	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Samoa, Pago Pago AS - Tide Gauge	100	0	line
330	spjy	spjy	-43.4333	146.9748	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	4	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Australia, Southport Jetty AU - Tide Gauge	1000	0	line
235	dhta	dhta	-46.922	160.562	NULL	NULL	t	slevel,stime	sea_level (m),time	\N	3	4	2	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Australia, Hobart - DART Buoy	100	0	line
238	djvw	djvw	-13.9908	110.097	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	3	4	2	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Australia, Dampier - DART Buoy	100	0	line
239	djvw2	djvw2	-13.9908	110.097	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	3	4	2	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Australia, Dampier - DART Buoy	100	0	line
243	dnzb2	dnzb2	-40.5992	179.0962	GNS Science ( New Zealand )	GNS Science ( New Zealand )	t	slevel,stime	sea_level (m),time	\N	3	4	2	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	New Zealand, E Coast - DART Buoy	100	0	line
262	dnzl	dnzl	-19.3096	166.782	GNS Science ( New Zealand )	GNS Science ( New Zealand )	t	slevel,stime	sea_level (m),time	\N	3	4	2	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	New Zealand - DART Buoy	100	0	line
263	dnzl2	dnzl2	-19.3096	166.782	GNS Science ( New Zealand )	GNS Science ( New Zealand )	t	slevel,stime	sea_level (m),time	\N	3	4	2	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	New Zealand - DART Buoy	100	0	line
264	dta2	dta2	-46.665	161.001	NULL	NULL	t	slevel,stime	sea_level (m),time	\N	3	4	2	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Australia, Hobart - DART Buoy	100	0	line
265	dta3	dta3	-44.8978	161.8412	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	3	4	2	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Australia, Hobart - DART Buoy	100	0	line
271	ferg	ferg	-19.2773	147.0584	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	4	4	2	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Australia, Cape Ferguson AU - Tide Gauge	100	0	line
278	gpab	gpab	-28.7631	114.5647	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	4	4	2	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Australia, Geraldton AU - Tide Gauge	100	0	line
279	groo	groo	-13.86	136.4158	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	4	4	2	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Australia, Groote Eylandt - Tide Gauge	100	0	line
287	kwaj2	kwaj2	8.7367	167.7383	National Ocean Service-NOAA ( USA )	National Ocean Service-NOAA ( USA )	t	slevel,stime	sea_level (m),time	\N	4	4	2	7	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Marshall Is, Kwajalein - Tide Gauge	100	0	line
288	lata	lata	-10.7208	165.8019	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	4	4	2	13	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Solomon Is, Lata Wharf SB - Tide Gauge	100	0	line
299	mrig	mrig	15.547938	-61.283093	Dominica Meteorological Service ( Dominica )	Dominica Meteorological Service ( Dominica )	t	slevel,stime	sea_level (m),time	\N	4	4	2	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Dominica Is, Marigot - Tide Gauge	100	0	line
301	naur	naur	-0.52	166.91	Japan Meteorological Agency	Japan Meteorological Agency	t	slevel,stime	sea_level (m),time	\N	4	4	2	8	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Nauru -Tide Gauge	100	0	line
305	niue	niue	-19.0525	-169.9214	University of Hawaii Sea Level Center ( USA )	University of Hawaii Sea Level Center ( USA )	t	slevel,stime	sea_level (m),time	\N	4	4	2	9	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Niue - Tide Gauge	100	0	line
311	pago2	pago2	-14.2766	-170.6907	National Ocean Service-NOAA ( USA )	National Ocean Service-NOAA ( USA )	t	slevel,stime	sea_level (m),time	\N	4	4	2	18	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Samoa, Pago Pago AS - Tide Gauge	100	0	line
314	phcp	phcp	-20.04	118.42	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	4	4	2	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Australia, Port Hedland AU - Tide Gauge 	100	0	line
318	pors	pors	-35.1086	138.467	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	4	4	2	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Australia, Port Stanavac AU - Tide Gauge	100	0	line
321	puyt	puyt	-46.0847836	166.5894083	Land Information New Zealand ( New Zealand )	Land Information New Zealand ( New Zealand )	t	slevel,stime	sea_level (m),time	\N	4	4	2	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	New Zealand, Puysegur Welcome Bay - Tide Gauge 	100	0	line
324	raro2	raro2	-21.2049	-159.7852	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	4	4	2	16	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Cook Is, Avatiu Rarotonga Harbour Wharf - Tide Gauge	100	0	line
332	sumt	sumt	-43.5696	172.7732	Land Information New Zealand ( New Zealand )	Land Information New Zealand ( New Zealand )	t	slevel,stime	sea_level (m),time	\N	4	4	2	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	New Zealand, Christchurch - Tide Gauge	100	0	line
334	tare	tare	-6.6928	156.4086	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	4	4	2	13	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Solomon Is, Tarekukure Wharf SB - Tide Gauge 	100	0	line
341	vati	vati	-17.3978066	177.7611542	Fiji Meteorological Service ( Fiji )	Fiji Meteorological Service ( Fiji )	t	slevel,stime	sea_level (m),time	\N	4	4	2	2	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Fiji, Vatia, Viti Levu - Tide Gauge 	100	0	line
343	wait	wait	-43.9458	-176.5608	Pacific Tsunami Warning Center ( USA )	Pacific Tsunami Warning Center ( USA )	t	slevel,stime	sea_level (m),time	\N	4	4	2	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	New Zealand, Waitangi, Chatham NZ - Tide Gauge	100	0	line
331	sprg	sprg	-42.5459	147.9327	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	4	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Australia, Spring Bay AU - Tide Gauge	1000	0	line
336	taut	taut	-37.6410885	176.1811791	Land Information New Zealand ( New Zealand )	Land Information New Zealand ( New Zealand )	t	slevel,stime	sea_level (m),time	\N	4	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	New Zealand, Port of Tauranga - Tide Gauge 	1000	0	line
344	wlgt	wlgt	-41.2846217	174.7790967	Land Information New Zealand ( New Zealand )	Land Information New Zealand ( New Zealand )	t	slevel,stime	sea_level (m),time	\N	4	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	New Zealand, Wellington - Tide Gauge	1000	0	line
208	alofi	alofi	-19.0527	-169.9209	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	4	4	1	9	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Niue, Alofi - Tide Gauge	1000	0	line
317	porl	porl	-38.3434	141.6132	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	4	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Australia, Portland AU - Tide Gauge 	1000	0	line
325	rbct	rbct	-29.280016	-177.8944	Land Information New Zealand ( New Zealand )	Land Information New Zealand ( New Zealand )	t	slevel,stime	sea_level (m),time	\N	4	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	New Zealand, Raoul Island Boat Cove - Tide Gauge	1000	0	line
333	tara	tara	1.3625	172.93	Kiribati Met Office ( Kiribati )	Kiribati Met Office ( Kiribati )	t	slevel,stime	sea_level (m),time	\N	4	4	1	6	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Kiribati, Betio, Tarawa - Tide Gauge 	1000	0	line
337	tbwc	tbwc	-37.1003	149.9266	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	4	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Australia, Twofold Bay AU - Tide Gauge	1000	0	line
346	yapi	yapi	9.5142	138.1246	Yap Weather Service Office ( Federated States of Micronesia )	Yap Weather Service Office ( Federated States of Micronesia )	t	slevel,stime	sea_level (m),time	\N	4	4	2	5	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&includesensors%5B%5D=rad&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Micronesia, Yap Island - Tide Gauge	100	0	line
339	trst	trst	-10.5865	142.2219	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	4	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Australia, Torres Strait AU - Tide Gauge	1000	0	line
340	vanu	vanu	-17.7553	168.3077	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	4	4	1	14	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Vanuatu, Port Vila - Tide Gauge	1000	0	line
228	dcor	dcor	-15.7109	158.4644	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	3	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Australia, Townsville - DART Buoy	100	-3346.97	line
342	viti	viti	-18.1342	178.4236	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	National Tidal Centre/Australian Bureau of Meteorology ( Australia )	t	slevel,stime	sea_level (m),time	\N	4	4	1	2	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Fiji, Suva, Viti Levu FJ - Tide Gauge	1000	0	line
245	dnzc2	dnzc2	-38.2001	-179.7978	GNS Science ( New Zealand )	GNS Science ( New Zealand )	t	slevel,stime	sea_level (m),time	\N	3	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	New Zealand, E Coast - DART Buoy	100	-3545.43	line
247	dnzd2	dnzd2	-36.0999	178.6037	GNS Science ( New Zealand )	GNS Science ( New Zealand )	t	slevel,stime	sea_level (m),time	\N	3	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	New Zealand - DART Buoy	100	-2447.63	line
248	dnze	dnze	-36.0493	-177.708	GNS Science ( New Zealand )	GNS Science ( New Zealand )	t	slevel,stime	sea_level (m),time	\N	3	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	New Zealand, E Coast - DART Buoy	100	-5777.23	line
249	dnze2	dnze2	-36.0493	-177.708	GNS Science ( New Zealand )	GNS Science ( New Zealand )	t	slevel,stime	sea_level (m),time	\N	3	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	New Zealand, E Coast - DART Buoy	100	-5777.23	line
251	dnzf2	dnzf2	-29.6826	-175.0125	GNS Science ( New Zealand )	GNS Science ( New Zealand )	t	slevel,stime	sea_level (m),time	\N	3	4	1	0	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	New Zealand, Tonga - DART Buoy	100	-5064.68	line
349	nuku	nuku	-8.914847	-140.084683	University of Hawaii Sea Level Center ( USA )	University of Hawaii Sea Level Center ( USA )	t	slevel,stime	sea_level (m),time		4	4	1	2	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&includesensors%5B%5D=rad&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	-999	French Polynesia, Marquesas - Tide Gauge	1000	0	scatter
348	pape	pape	-17.533092	-149.572678	University of Hawaii Sea Level Center ( USA )	University of Hawaii Sea Level Center ( USA )	t	slevel,stime	sea_level (m),time		4	4	2	2	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&includesensors%5B%5D=rad&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	-999	French Polynesia, Tahiti - Tide Gauge	1000	0	line
345	xmas	xmas	1.984	-157.473	Kiribati Met Office ( Kiribati )	Kiribati Met Office ( Kiribati )	t	slevel,stime	sea_level (m),time	\N	4	4	1	6	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&includesensors%5B%5D=rad&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	\N	Kiribati, Christmas KI - Tide Gauge	1000	0	line
347	numbo	numbo	-22.2466	166.4116	Service hydrographique et océanographique de la marine ( France )	Service hydrographique et océanographique de la marine ( France )	t	slevel,stime	sea_level (m),time		4	4	1	20	https://api.ioc-sealevelmonitoring.org/v2/stations/STATION_ID/data?timestart=TIME_START&timestop=TIME_END&nofilter=false&allsensors=false&skip_gaps_until=TIME_END&limit=LIMIT_DATA	8	0	-999	New Caledonia Noumea - Tide Gauge	1000	0	line
368	ta_faga_cove	ta_faga_cove	-14.2453	-169.5087	PACIOOS	\N	t	time,sea_surface_wave_significant_height,sea_surface_wave_mean_period,sea_surface_wave_from_direction	time,sea_surface_wave_significant_height,sea_surface_wave_mean_period,sea_surface_wave_from_direction	\N	1	2	1	18	https://erddap.backyardbuoys.org/erddap/tabledap/backyardbuoys_ta_faga_cove.geoJson?time,latitude,longitude,sea_surface_wave_significant_height,sea_surface_wave_mean_period,sea_surface_wave_from_direction	\N	0	\N	ta_faga_cove	100	0	line
366	faleasao_harbor_smart	faleasao_harbor_smart	-14.2201	-169.5193	PACIOOS	\N	t	time,depth,sea_water_temperature,sea_water_temperature_qc_agg,sea_water_temperature_qc_gross_range_test,sea_water_temperature_qc_rate_of_change_test,sea_water_temperature_qc_spike_test,sea_water_temperature_qc_flat_line_test	time,depth,sea_water_temperature,sea_water_temperature_qc_agg,sea_water_temperature_qc_gross_range_test,sea_water_temperature_qc_rate_of_change_test,sea_water_temperature_qc_spike_test,sea_water_temperature_qc_flat_line_test	\N	1	6	1	18	https://erddap.backyardbuoys.org/erddap/tabledap/backyardbuoys_STATION_ID.json?&time>=START_TIME&time<=END_TIME	\N	0	\N	faleasao_harbor_smart	100	0	line
367	ta_faga_cove_smart	ta_faga_cove_smart	-14.2453	-169.5087	PACIOOS	\N	t	time,depth,sea_water_temperature,sea_water_temperature_qc_agg,sea_water_temperature_qc_gross_range_test,sea_water_temperature_qc_rate_of_change_test,sea_water_temperature_qc_spike_test,sea_water_temperature_qc_flat_line_test	time,depth,sea_water_temperature,sea_water_temperature_qc_agg,sea_water_temperature_qc_gross_range_test,sea_water_temperature_qc_rate_of_change_test,sea_water_temperature_qc_spike_test,sea_water_temperature_qc_flat_line_test	\N	1	6	1	18	https://erddap.backyardbuoys.org/erddap/tabledap/backyardbuoys_STATION_ID.json?&time>=START_TIME&time<=END_TIME	\N	0	\N	ta_faga_cove_smart	100	0	line
372	hagat_smart	hagat_smart	13.3488	144.6371	PACIOOS	\N	t	time,depth,sea_water_temperature,sea_water_temperature_qc_agg,sea_water_temperature_qc_gross_range_test,sea_water_temperature_qc_rate_of_change_test,sea_water_temperature_qc_spike_test,sea_water_temperature_qc_flat_line_test	time,depth,sea_water_temperature,sea_water_temperature_qc_agg,sea_water_temperature_qc_gross_range_test,sea_water_temperature_qc_rate_of_change_test,sea_water_temperature_qc_spike_test,sea_water_temperature_qc_flat_line_test	\N	1	6	1	24	https://erddap.backyardbuoys.org/erddap/tabledap/backyardbuoys_STATION_ID.json?&time>=START_TIME&time<=END_TIME	\N	0	\N	hagat_smart	100	0	line
375	majuro_smart	majuro_smart	7.1513	171.202	PACIOOS	\N	t	time,depth,sea_water_temperature,sea_water_temperature_qc_agg,sea_water_temperature_qc_gross_range_test,sea_water_temperature_qc_rate_of_change_test,sea_water_temperature_qc_spike_test,sea_water_temperature_qc_flat_line_test	time,depth,sea_water_temperature,sea_water_temperature_qc_agg,sea_water_temperature_qc_gross_range_test,sea_water_temperature_qc_rate_of_change_test,sea_water_temperature_qc_spike_test,sea_water_temperature_qc_flat_line_test	\N	1	6	1	7	https://erddap.backyardbuoys.org/erddap/tabledap/backyardbuoys_STATION_ID.json?&time>=START_TIME&time<=END_TIME	\N	0	\N	majuro_smart	100	0	line
369	tau_harbor	tau_harbor	-14.243	-169.5111	PACIOOS	\N	t	time,sea_surface_wave_significant_height,sea_surface_wave_mean_period,sea_surface_wave_from_direction	time,sea_surface_wave_significant_height,sea_surface_wave_mean_period,sea_surface_wave_from_direction	\N	1	2	1	18	https://erddap.backyardbuoys.org/erddap/tabledap/backyardbuoys_tau_harbor.geoJson?time,latitude,longitude,sea_surface_wave_significant_height,sea_surface_wave_mean_period,sea_surface_wave_from_direction	\N	0	\N	tau_harbor	100	0	line
371	hagat	hagat	13.3488	144.6386	PACIOOS	\N	t	time,sea_surface_wave_significant_height,sea_surface_wave_mean_period,sea_surface_wave_from_direction	time,sea_surface_wave_significant_height,sea_surface_wave_mean_period,sea_surface_wave_from_direction	\N	1	2	1	24	https://erddap.backyardbuoys.org/erddap/tabledap/backyardbuoys_hagat.geoJson?time,latitude,longitude,sea_surface_wave_significant_height,sea_surface_wave_mean_period,sea_surface_wave_from_direction	\N	0	\N	hagat	100	0	line
373	malesso	malesso	13.2709	144.6575	PACIOOS	\N	t	time,sea_surface_wave_significant_height,sea_surface_wave_mean_period,sea_surface_wave_from_direction	time,sea_surface_wave_significant_height,sea_surface_wave_mean_period,sea_surface_wave_from_direction	\N	1	2	1	24	https://erddap.backyardbuoys.org/erddap/tabledap/backyardbuoys_malesso.geoJson?time,latitude,longitude,sea_surface_wave_significant_height,sea_surface_wave_mean_period,sea_surface_wave_from_direction	\N	0	\N	malesso	100	0	line
370	tau_harbor_smart	tau_harbor_smart	-14.243	-169.5111	PACIOOS	\N	t	time,depth,sea_water_temperature,sea_water_temperature_qc_agg,sea_water_temperature_qc_gross_range_test,sea_water_temperature_qc_rate_of_change_test,sea_water_temperature_qc_spike_test,sea_water_temperature_qc_flat_line_test	time,depth,sea_water_temperature,sea_water_temperature_qc_agg,sea_water_temperature_qc_gross_range_test,sea_water_temperature_qc_rate_of_change_test,sea_water_temperature_qc_spike_test,sea_water_temperature_qc_flat_line_test	\N	1	6	1	18	https://erddap.backyardbuoys.org/erddap/tabledap/backyardbuoys_STATION_ID.json?&time>=START_TIME&time<=END_TIME	\N	0	\N	tau_harbor_smart	100	0	line
374	majuro	majuro	7.1492	171.1998	PACIOOS	\N	t	time,sea_surface_wave_significant_height,sea_surface_wave_mean_period,sea_surface_wave_from_direction	time,sea_surface_wave_significant_height,sea_surface_wave_mean_period,sea_surface_wave_from_direction	\N	1	2	1	7	https://erddap.backyardbuoys.org/erddap/tabledap/backyardbuoys_majuro.geoJson?time,latitude,longitude,sea_surface_wave_significant_height,sea_surface_wave_mean_period,sea_surface_wave_from_direction	\N	0	\N	majuro	100	0	line
376	pehlung	pehlung	6.8423	158.0807	PACIOOS	\N	t	time,sea_surface_wave_significant_height,sea_surface_wave_mean_period,sea_surface_wave_from_direction	time,sea_surface_wave_significant_height,sea_surface_wave_mean_period,sea_surface_wave_from_direction	\N	1	2	1	5	https://erddap.backyardbuoys.org/erddap/tabledap/backyardbuoys_pehlung.geoJson?time,latitude,longitude,sea_surface_wave_significant_height,sea_surface_wave_mean_period,sea_surface_wave_from_direction	\N	0	\N	pehlung	100	0	line
377	yongraq27	yongraq27	9.4193	138.0404	PACIOOS	\N	t	time,sea_surface_wave_significant_height,sea_surface_wave_mean_period,sea_surface_wave_from_direction	time,sea_surface_wave_significant_height,sea_surface_wave_mean_period,sea_surface_wave_from_direction	\N	1	2	1	5	https://erddap.backyardbuoys.org/erddap/tabledap/backyardbuoys_yongraq27.geoJson?time,latitude,longitude,sea_surface_wave_significant_height,sea_surface_wave_mean_period,sea_surface_wave_from_direction	\N	0	\N	yongraq27	100	0	line
379	akino_reef_saipan_smart	akino_reef_saipan_smart	15.221	145.7015	PACIOOS	\N	t	time,depth,sea_water_temperature,sea_water_temperature_qc_agg,sea_water_temperature_qc_gross_range_test,sea_water_temperature_qc_rate_of_change_test,sea_water_temperature_qc_spike_test,sea_water_temperature_qc_flat_line_test	time,depth,sea_water_temperature,sea_water_temperature_qc_agg,sea_water_temperature_qc_gross_range_test,sea_water_temperature_qc_rate_of_change_test,sea_water_temperature_qc_spike_test,sea_water_temperature_qc_flat_line_test	\N	1	6	1	23	https://erddap.backyardbuoys.org/erddap/tabledap/backyardbuoys_STATION_ID.json?&time>=START_TIME&time<=END_TIME	\N	0	\N	akino_reef_saipan_smart	100	0	line
380	saipan_sugar_dock	saipan_sugar_dock	15.15	145.6944	PACIOOS	\N	t	time,sea_surface_wave_significant_height,sea_surface_wave_mean_period,sea_surface_wave_from_direction	time,sea_surface_wave_significant_height,sea_surface_wave_mean_period,sea_surface_wave_from_direction	\N	1	2	1	23	https://erddap.backyardbuoys.org/erddap/tabledap/backyardbuoys_saipan_sugar_dock.geoJson?time,latitude,longitude,sea_surface_wave_significant_height,sea_surface_wave_mean_period,sea_surface_wave_from_direction	\N	0	\N	saipan_sugar_dock	100	0	line
378	akino_reef_saipan	akino_reef_saipan	15.2208	145.7015	PACIOOS	\N	t	time,sea_surface_wave_significant_height,sea_surface_wave_mean_period,sea_surface_wave_from_direction	time,sea_surface_wave_significant_height,sea_surface_wave_mean_period,sea_surface_wave_from_direction	\N	1	2	1	23	https://erddap.backyardbuoys.org/erddap/tabledap/backyardbuoys_akino_reef_saipan.geoJson?time,latitude,longitude,sea_surface_wave_significant_height,sea_surface_wave_mean_period,sea_surface_wave_from_direction	\N	0	\N	akino_reef_saipan	100	0	line
381	SPOT-31090C	SPOT-31090C	-10.80708	179.45052	TMS	\N	t	significantWaveHeight,peakPeriod,peakDirection,timestamp	significant_wave_height,peak_wave_period,peak_wave_direction,time	\N	1	1	1	4	https://wavefleet.spoondriftspotter.co/api/wave-data?spotterId=SPOT-31090C&token=REPALCE_TOKEN_STRING&includeWaves=true&includeTrack=true&includeDirectionalMoments=true&limit=500	6	0	\N	Tuvalu, Funafuti, Wavebuoy	100	0	line
384	SPOT-30982C	SPOT-30982C	-8.52545	179.1966333	TMS	\N	f	significantWaveHeight,peakPeriod,peakDirection,timestamp	significant_wave_height,peak_wave_period,peak_wave_direction,time	\N	1	1	1	4	https://wavefleet.spoondriftspotter.co/api/wave-data?spotterId=SPOT-30982C&token=REPALCE_TOKEN_STRING&includeWaves=true&includeTrack=true&includeDirectionalMoments=true&limit=500	6	0	\N	Tuvalu, Funafuti, Wavebuoy	100	0	line
382	SPOT-1419	SPOT-1419	-6.2971167	176.3064	TMS	\N	t	significantWaveHeight,peakPeriod,peakDirection,timestamp	significant_wave_height,peak_wave_period,peak_wave_direction,time	\N	1	1	1	4	https://wavefleet.spoondriftspotter.co/api/wave-data?spotterId=SPOT-1419&token=REPALCE_TOKEN_STRING&includeWaves=true&includeTrack=true&includeDirectionalMoments=true&limit=500	6	0	\N	Tuvalu, Funafuti, Wavebuoy	100	0	line
\.


--
-- TOC entry 4907 (class 0 OID 65639)
-- Dependencies: 221
-- Data for Name: status; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.status (id, value) FROM stdin;
1	active
2	inactive
3	deleted
\.


--
-- TOC entry 4909 (class 0 OID 65645)
-- Dependencies: 223
-- Data for Name: token; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.token (id, token, comments) FROM stdin;
2	b9f2c081116e70f44152dd9aa45dcb	FMS
3	e62e5e58efac587d2c7eb4a1d938b0	KMS
4	e5c7ab12898f4414c0acf817b4bbde	SamoaMet
5	743acb9023dec1ef847d5651596352	TongaMet
6	99a920305541f1c38db611ebab95ba	TMS
7	2a348598f294c6b0ce5f7e41e5c0f5	NMS
8	bffa4c2e02b85db7fa813d2ea69fa8cfcae5885d23a0ec47edf4382314c803d136d5ff69ffe960f426beae8d1f1f4fba7c6a4c6219b785d254a4a70369713041	IOC
11	TMS_Sealevel_SPC/Engulf3-Oat-Chamomile	neon_cred
1	c10023d447b508d32a51d1602ba6fb	SPC
10	hRs3HvsoqyEte+wD/bAkcgnYux5pydRtqHkI0QNZR0M=	neon_token
12	kdwtw5roiq3dzjtxn1nimb2q	neon_cookie
\.


--
-- TOC entry 4911 (class 0 OID 65651)
-- Dependencies: 225
-- Data for Name: type; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.type (id, value) FROM stdin;
1	Wave Buoy
3	DART Buoy
4	Tide Gauge
\.


--
-- TOC entry 4927 (class 0 OID 0)
-- Dependencies: 218
-- Name: access_method_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.access_method_id_seq', 6, true);


--
-- TOC entry 4928 (class 0 OID 0)
-- Dependencies: 220
-- Name: station_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.station_id_seq', 384, true);


--
-- TOC entry 4929 (class 0 OID 0)
-- Dependencies: 222
-- Name: status_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.status_id_seq', 3, true);


--
-- TOC entry 4930 (class 0 OID 0)
-- Dependencies: 224
-- Name: token_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.token_id_seq', 12, true);


--
-- TOC entry 4931 (class 0 OID 0)
-- Dependencies: 226
-- Name: type_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.type_id_seq', 8, true);


--
-- TOC entry 4725 (class 2606 OID 65663)
-- Name: access_method access_method_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.access_method
    ADD CONSTRAINT access_method_pkey PRIMARY KEY (id);


--
-- TOC entry 4740 (class 2606 OID 65665)
-- Name: station station_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.station
    ADD CONSTRAINT station_pkey PRIMARY KEY (id);


--
-- TOC entry 4744 (class 2606 OID 65667)
-- Name: status status_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.status
    ADD CONSTRAINT status_pkey PRIMARY KEY (id);


--
-- TOC entry 4747 (class 2606 OID 65669)
-- Name: token token_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.token
    ADD CONSTRAINT token_pkey PRIMARY KEY (id);


--
-- TOC entry 4749 (class 2606 OID 65671)
-- Name: token token_token_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.token
    ADD CONSTRAINT token_token_key UNIQUE (token);


--
-- TOC entry 4753 (class 2606 OID 65673)
-- Name: type type_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.type
    ADD CONSTRAINT type_pkey PRIMARY KEY (id);


--
-- TOC entry 4728 (class 1259 OID 65674)
-- Name: idx_station_token_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_station_token_id ON public.station USING btree (token_id);


--
-- TOC entry 4745 (class 1259 OID 65675)
-- Name: idx_token_token; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_token_token ON public.token USING btree (token);


--
-- TOC entry 4726 (class 1259 OID 65676)
-- Name: ix_access_method_function; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX ix_access_method_function ON public.access_method USING btree (function);


--
-- TOC entry 4727 (class 1259 OID 65677)
-- Name: ix_access_method_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_access_method_id ON public.access_method USING btree (id);


--
-- TOC entry 4729 (class 1259 OID 65678)
-- Name: ix_station_description; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_station_description ON public.station USING btree (description);


--
-- TOC entry 4730 (class 1259 OID 65679)
-- Name: ix_station_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_station_id ON public.station USING btree (id);


--
-- TOC entry 4731 (class 1259 OID 65680)
-- Name: ix_station_latitude; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_station_latitude ON public.station USING btree (latitude);


--
-- TOC entry 4732 (class 1259 OID 65681)
-- Name: ix_station_longitude; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_station_longitude ON public.station USING btree (longitude);


--
-- TOC entry 4733 (class 1259 OID 65682)
-- Name: ix_station_maintainer; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_station_maintainer ON public.station USING btree (maintainer);


--
-- TOC entry 4734 (class 1259 OID 65683)
-- Name: ix_station_owner; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_station_owner ON public.station USING btree (owner);


--
-- TOC entry 4735 (class 1259 OID 65684)
-- Name: ix_station_project; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_station_project ON public.station USING btree (project);


--
-- TOC entry 4736 (class 1259 OID 65685)
-- Name: ix_station_station_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_station_station_id ON public.station USING btree (station_id);


--
-- TOC entry 4737 (class 1259 OID 65686)
-- Name: ix_station_variable_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_station_variable_id ON public.station USING btree (variable_id);


--
-- TOC entry 4738 (class 1259 OID 65687)
-- Name: ix_station_variable_label; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_station_variable_label ON public.station USING btree (variable_label);


--
-- TOC entry 4741 (class 1259 OID 65688)
-- Name: ix_status_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_status_id ON public.status USING btree (id);


--
-- TOC entry 4742 (class 1259 OID 65689)
-- Name: ix_status_value; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX ix_status_value ON public.status USING btree (value);


--
-- TOC entry 4750 (class 1259 OID 65690)
-- Name: ix_type_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_type_id ON public.type USING btree (id);


--
-- TOC entry 4751 (class 1259 OID 65691)
-- Name: ix_type_value; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX ix_type_value ON public.type USING btree (value);


--
-- TOC entry 4754 (class 2606 OID 65692)
-- Name: station station_access_method_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.station
    ADD CONSTRAINT station_access_method_id_fkey FOREIGN KEY (access_method_id) REFERENCES public.access_method(id);


--
-- TOC entry 4755 (class 2606 OID 65697)
-- Name: station station_status_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.station
    ADD CONSTRAINT station_status_id_fkey FOREIGN KEY (status_id) REFERENCES public.status(id);


--
-- TOC entry 4756 (class 2606 OID 65702)
-- Name: station station_token_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.station
    ADD CONSTRAINT station_token_id_fkey FOREIGN KEY (token_id) REFERENCES public.token(id);


--
-- TOC entry 4757 (class 2606 OID 65707)
-- Name: station station_type_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.station
    ADD CONSTRAINT station_type_id_fkey FOREIGN KEY (type_id) REFERENCES public.type(id);


-- Completed on 2026-07-09 10:53:16

--
-- PostgreSQL database dump complete
--

