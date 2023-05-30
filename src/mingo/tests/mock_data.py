from platform import system
from tempfile import gettempdir
from pathlib import Path
import pytest
from mingo import Database
from typing import Union, Iterable

# DATA FOR MOCK SOURCE FILES

MOCK_SOURCE_DATA = {
    "10-16-800": """HEADER
    CASE
        Particle []: gamma (0), electron (1), muon (2), neutron (3), proton (4)
        Number of events []:
        Emin [MeV]:
        Emax [MeV]:
        E distribution: constant (0), gaussian (1), exponential (2)
        Theta min [deg]:
        Theta max [deg]:
        Detector plane - X dimension [mm]:
        Detector plane - Y dimension [mm]:
        Detector plane - Z dimension [mm]:
    ACTIVE PLANES
        Plane 1 - Z coordinate [mm]: 0 by definition
        Plane 2 - Z coordinate [mm]:
        Plane 3 - Z coordinate [mm]:
        Plane 4 - Z coordinate [mm]:
    PASSIVE PLANES
        Plane 1 - Z coordinate [mm]: Measured downwards from first active plane
        Plane 2 - Z coordinate [mm]: Measured downwards from first active plane
        Plane 3 - Z coordinate [mm]: Measured downwards from first active plane
        Plane 4 - Z coordinate [mm]: Measured downwards from first active plane
        Plane 1 - Thickness [mm]:
        Plane 2 - Thickness [mm]:
        Plane 3 - Thickness [mm]:
        Plane 4 - Thickness [mm]:
        Plane 1 - Material []: Pb (0), Fe (1), W (2), Polyethylene (3)
        Plane 2 - Material []: Pb (0), Fe (1), W (2), Polyethylene (3)
        Plane 3 - Material []: Pb (0), Fe (1), W (2), Polyethylene (3)
        Plane 4 - Material []: Pb (0), Fe (1), W (2), Polyethylene (3)
   EVENT
        Event number []:
        Initial energy [MeV]:
        Initial X [mm]: Measured from center of plane, positive to the right
        Initial Y [mm]: Measured from center of plane, right handed frame
        Initial Z [mm]: 0 by definition
        Initial theta [deg]:
        Initial phi [deg]:
        Number of hits []:
    HIT
        Plane number []: First is 1, last is 4
        X [mm]: Measured from center of plane, positive to the right
        Y [mm]: Measured from center of plane, right handed frame
        Z [mm]: Measured downwards from first active plane
        Time since first impact [ns]:
DATA
1	10000	800	800	0	0	0	999	999	22
0	100	200	400
null	22	null	222	null	10.4	null	16.2	null	0	null	0
1	800 	0.00000	0.00000	0.00000	0	0	24
1	0.00000	0.00000	0.00000	0
2	-0.71720	-9.39800	100.00000	0.3692
3	-14.78000	-26.62000	200.00000	0.7113
4	-122.10000	46.55000	420.20000	1.589
3	47.82000	-10.86000	216.90000	0.901
4	-100.70000	-186.50000	419.50000	1.767
4	-44.30000	-322.90000	400.00000	2.005
2	-9.68600	-2.48300	100.00000	0.3695
3	-32.41000	32.50000	200.00000	0.7339
3	0.00046	22.88000	200.50000	0.8806
2	-0.97830	-7.40000	100.00000	0.3683
3	-9.57700	-24.90000	200.00000	0.7084
3	-9.66400	-25.09000	201.10000	0.712
2	-1.49600	0.55360	100.00000	0.367
3	0.88750	6.63500	200.00000	0.7014
3	-4.35600	1.73800	221.10000	0.7711
3	-4.35600	1.73800	221.10000	0.7711
3	-4.35600	1.73800	221.10000	0.7711
4	102.80000	-154.90000	400.00000	1.65
2	206.70000	-33.64000	112.30000	1.626
3	135.20000	-11.92000	209.30000	1.218
3	0.00259	2.35100	206.40000	0.7218
2	68.31000	-21.56000	100.00000	0.4779
2	23.83000	3.95000	100.00000	0.3809
""",
    "10-16-1000": """HEADER
    CASE
        Particle []: gamma (0), electron (1), muon (2), neutron (3), proton (4)
        Number of events []:
        Emin [MeV]:
        Emax [MeV]:
        E distribution: constant (0), gaussian (1), exponential (2)
        Theta min [deg]:
        Theta max [deg]:
        Detector plane - X dimension [mm]:
        Detector plane - Y dimension [mm]:
        Detector plane - Z dimension [mm]:
    ACTIVE PLANES
        Plane 1 - Z coordinate [mm]: 0 by definition
        Plane 2 - Z coordinate [mm]:
        Plane 3 - Z coordinate [mm]:
        Plane 4 - Z coordinate [mm]:
    PASSIVE PLANES
        Plane 1 - Z coordinate [mm]: Measured downwards from first active plane
        Plane 2 - Z coordinate [mm]: Measured downwards from first active plane
        Plane 3 - Z coordinate [mm]: Measured downwards from first active plane
        Plane 4 - Z coordinate [mm]: Measured downwards from first active plane
        Plane 1 - Thickness [mm]:
        Plane 2 - Thickness [mm]:
        Plane 3 - Thickness [mm]:
        Plane 4 - Thickness [mm]:
        Plane 1 - Material []: Pb (0), Fe (1), W (2), Polyethylene (3)
        Plane 2 - Material []: Pb (0), Fe (1), W (2), Polyethylene (3)
        Plane 3 - Material []: Pb (0), Fe (1), W (2), Polyethylene (3)
        Plane 4 - Material []: Pb (0), Fe (1), W (2), Polyethylene (3)
   EVENT
        Event number []:
        Initial energy [MeV]:
        Initial X [mm]: Measured from center of plane, positive to the right
        Initial Y [mm]: Measured from center of plane, right handed frame
        Initial Z [mm]: 0 by definition
        Initial theta [deg]:
        Initial phi [deg]:
        Number of hits []:
    HIT
        Plane number []: First is 1, last is 4
        X [mm]: Measured from center of plane, positive to the right
        Y [mm]: Measured from center of plane, right handed frame
        Z [mm]: Measured downwards from first active plane
        Time since first impact [ns]:
DATA
1	10000	1000	1000	0	0	0	999	999	22
0	100	200	400
null	22	null	222	null	10.4	null	16.2	null	0	null	0
1	1000 	0.00000	0.00000	0.00000	0	0	42
1	0.00000	0.00000	0.00000	0
2	1.09100	9.11700	100.00000	0.3689
3	5.64400	21.04000	200.00000	0.7052
2	-363.60000	50.83000	101.10000	2.101
2	-333.50000	49.61000	108.50000	1.997
3	19.63000	24.74000	201.70000	1.002
3	28.52000	14.60000	215.00000	0.9386
4	-174.70000	101.40000	416.40000	1.753
4	-173.20000	100.40000	413.50000	1.742
4	-168.20000	102.60000	414.80000	1.723
4	-165.50000	105.00000	418.10000	1.707
2	-1.00800	4.80300	100.00000	0.3675
3	-6.35000	10.19000	200.00000	0.7028
2	-2.48000	3.93200	100.00000	0.3674
3	-11.83000	2.12800	200.00000	0.7028
4	-210.20000	67.42000	412.70000	1.722
2	0.43220	1.28000	100.00000	0.367
3	2.06200	3.81100	200.00000	0.7007
4	18.94000	-20.78000	407.50000	1.401
4	18.94000	-20.78000	407.50000	1.401
4	18.94000	-20.78000	407.50000	1.401
4	13.26000	10.33000	400.00000	1.37
4	-168.80000	-23.07000	413.50000	1.649
4	-165.80000	-25.09000	410.60000	1.634
4	133.10000	126.80000	400.00000	1.633
4	124.90000	137.80000	408.40000	1.727
4	133.00000	129.10000	411.90000	1.685
4	289.80000	80.85000	400.00000	1.962
4	70.94000	-34.96000	400.00000	1.429
2	3.95400	0.05303	100.00000	0.3673
3	11.97000	2.92200	200.00000	0.7021
4	-13.08000	24.54000	400.90000	1.479
4	-26.07000	31.53000	401.20000	1.43
4	-27.64000	32.53000	406.60000	1.411
2	12.91000	-20.62000	101.50000	1.294
2	-24.30000	16.27000	100.00000	0.3868
3	-52.48000	56.93000	204.30000	0.772
3	-52.48000	56.93000	204.30000	0.772
3	-163.80000	67.02000	208.90000	1.854
2	-35.67000	98.33000	100.60000	1.285
3	-50.38000	56.04000	212.90000	0.8269
3	-52.48000	56.93000	204.30000	0.772
""",
    "16-10-800": """HEADER
    CASE
        Particle []: gamma (0), electron (1), muon (2), neutron (3), proton (4)
        Number of events []:
        Emin [MeV]:
        Emax [MeV]:
        E distribution: constant (0), gaussian (1), exponential (2)
        Theta min [deg]:
        Theta max [deg]:
        Detector plane - X dimension [mm]:
        Detector plane - Y dimension [mm]:
        Detector plane - Z dimension [mm]:
    ACTIVE PLANES
        Plane 1 - Z coordinate [mm]: 0 by definition
        Plane 2 - Z coordinate [mm]:
        Plane 3 - Z coordinate [mm]:
        Plane 4 - Z coordinate [mm]:
    PASSIVE PLANES
        Plane 1 - Z coordinate [mm]: Measured downwards from first active plane
        Plane 2 - Z coordinate [mm]: Measured downwards from first active plane
        Plane 3 - Z coordinate [mm]: Measured downwards from first active plane
        Plane 4 - Z coordinate [mm]: Measured downwards from first active plane
        Plane 1 - Thickness [mm]:
        Plane 2 - Thickness [mm]:
        Plane 3 - Thickness [mm]:
        Plane 4 - Thickness [mm]:
        Plane 1 - Material []: Pb (0), Fe (1), W (2), Polyethylene (3)
        Plane 2 - Material []: Pb (0), Fe (1), W (2), Polyethylene (3)
        Plane 3 - Material []: Pb (0), Fe (1), W (2), Polyethylene (3)
        Plane 4 - Material []: Pb (0), Fe (1), W (2), Polyethylene (3)
   EVENT
        Event number []:
        Initial energy [MeV]:
        Initial X [mm]: Measured from center of plane, positive to the right
        Initial Y [mm]: Measured from center of plane, right handed frame
        Initial Z [mm]: 0 by definition
        Initial theta [deg]:
        Initial phi [deg]:
        Number of hits []:
    HIT
        Plane number []: First is 1, last is 4
        X [mm]: Measured from center of plane, positive to the right
        Y [mm]: Measured from center of plane, right handed frame
        Z [mm]: Measured downwards from first active plane
        Time since first impact [ns]:
DATA
1	10000	800	800	0	0	0	999	999	22
0	100	200	400
null	22	null	222	null	16.2	null	10.4	null	0	null	0
1	800 	0.00000	0.00000	0.00000	0	0	47
1	0.00000	0.00000	0.00000	0
2	-2.00000	-16.74000	100.00000	0.3742
3	-4.19000	-48.21000	205.40000	0.7412
3	-4.19000	-48.21000	205.40000	0.7412
3	-28.68000	42.09000	210.40000	1.136
3	-4.19000	-48.21000	205.40000	0.7412
4	47.22000	-89.60000	410.70000	1.469
2	16.95000	-33.24000	100.00000	0.3998
2	12.54000	-37.23000	111.40000	0.4578
3	51.99000	-119.30000	209.80000	0.9086
3	53.68000	-117.00000	203.20000	0.8848
2	97.36000	106.70000	112.00000	0.6884
2	97.36000	106.70000	112.00000	0.6884
2	97.36000	106.70000	112.00000	0.6884
2	-52.86000	-11.45000	100.00000	0.4448
3	-61.34000	-57.71000	206.80000	0.8669
2	-14.67000	12.65000	100.00000	0.3771
2	-35.80000	9.72400	111.70000	0.6317
2	-27.91000	1.82800	112.80000	0.5943
2	-33.50000	30.39000	100.30000	0.4886
2	-2.29600	11.64000	120.70000	0.4385
2	-2.29600	11.64000	120.70000	0.4385
3	-89.66000	-42.27000	200.00000	0.8868
2	160.30000	-394.30000	112.00000	2.444
2	145.80000	-402.80000	119.30000	2.383
2	145.50000	-404.10000	119.10000	2.379
2	142.40000	-388.40000	114.20000	2.323
2	-239.60000	94.74000	105.50000	1.67
3	-99.37000	-25.71000	205.70000	0.9681
2	-2.29600	11.64000	120.70000	0.4385
3	5.24700	19.09000	200.00000	0.7053
2	62.34000	-23.97000	109.70000	0.4995
2	10.17000	19.74000	100.00000	0.3795
3	54.71000	64.52000	200.00000	0.7766
2	-21.84000	-22.98000	100.00000	0.3912
3	-53.52000	0.33960	200.00000	0.7607
3	-58.38000	-7.06500	222.00000	0.8451
2	-130.80000	60.93000	118.90000	1.574
3	-21.44000	34.74000	203.90000	1.104
3	-58.97000	-6.73700	205.20000	0.917
3	-61.51000	-4.60000	214.80000	0.8833
2	2.92500	-43.11000	100.00000	0.4135
2	50.13000	-19.77000	111.80000	0.6178
2	30.92000	-33.79000	116.60000	0.5369
2	-4.02100	-40.51000	116.60000	0.5082
2	-1.64800	-46.70000	101.00000	0.4518
2	-22.70000	66.00000	107.40000	1.279
""",
    "16-10-1000": """HEADER
    CASE
        Particle []: gamma (0), electron (1), muon (2), neutron (3), proton (4)
        Number of events []:
        Emin [MeV]:
        Emax [MeV]:
        E distribution: constant (0), gaussian (1), exponential (2)
        Theta min [deg]:
        Theta max [deg]:
        Detector plane - X dimension [mm]:
        Detector plane - Y dimension [mm]:
        Detector plane - Z dimension [mm]:
    ACTIVE PLANES
        Plane 1 - Z coordinate [mm]: 0 by definition
        Plane 2 - Z coordinate [mm]:
        Plane 3 - Z coordinate [mm]:
        Plane 4 - Z coordinate [mm]:
    PASSIVE PLANES
        Plane 1 - Z coordinate [mm]: Measured downwards from first active plane
        Plane 2 - Z coordinate [mm]: Measured downwards from first active plane
        Plane 3 - Z coordinate [mm]: Measured downwards from first active plane
        Plane 4 - Z coordinate [mm]: Measured downwards from first active plane
        Plane 1 - Thickness [mm]:
        Plane 2 - Thickness [mm]:
        Plane 3 - Thickness [mm]:
        Plane 4 - Thickness [mm]:
        Plane 1 - Material []: Pb (0), Fe (1), W (2), Polyethylene (3)
        Plane 2 - Material []: Pb (0), Fe (1), W (2), Polyethylene (3)
        Plane 3 - Material []: Pb (0), Fe (1), W (2), Polyethylene (3)
        Plane 4 - Material []: Pb (0), Fe (1), W (2), Polyethylene (3)
   EVENT
        Event number []:
        Initial energy [MeV]:
        Initial X [mm]: Measured from center of plane, positive to the right
        Initial Y [mm]: Measured from center of plane, right handed frame
        Initial Z [mm]: 0 by definition
        Initial theta [deg]:
        Initial phi [deg]:
        Number of hits []:
    HIT
        Plane number []: First is 1, last is 4
        X [mm]: Measured from center of plane, positive to the right
        Y [mm]: Measured from center of plane, right handed frame
        Z [mm]: Measured downwards from first active plane
        Time since first impact [ns]:
DATA
1	10000	1000	1000	0	0	0	999	999	22
0	100	200	400
null	22	null	222	null	16.2	null	10.4	null	0	null	0
1	1000 	0.00000	0.00000	0.00000	0	0	31
1	0.00000	0.00000	0.00000	0
2	-5.11200	4.88600	100.00000	0.3683
3	-27.88000	13.09000	200.00000	0.712
4	-62.27000	26.43000	411.20000	1.427
4	-62.27000	26.43000	411.20000	1.427
4	-62.27000	26.43000	411.20000	1.427
2	7.80700	-6.87500	100.00000	0.3744
3	-11.00000	103.00000	214.60000	1.106
3	-10.58000	103.00000	214.50000	1.105
3	-4.25500	103.30000	202.60000	1.06
2	-20.78000	13.04000	112.20000	0.6301
2	-18.44000	5.18600	118.30000	0.5961
2	-13.21000	-22.59000	119.30000	0.5018
3	177.00000	64.76000	200.90000	1.777
2	51.30000	102.70000	120.50000	1.264
2	22.53000	-1.50700	100.00000	0.3801
4	170.90000	8.42100	412.30000	1.596
2	25.34000	-27.46000	100.00000	0.4014
4	118.80000	-68.44000	404.50000	1.954
4	113.40000	-50.34000	416.10000	1.88
4	193.60000	-70.61000	414.20000	1.602
2	-2.80400	-1.11100	100.00000	0.3672
3	-111.00000	42.18000	200.00000	0.8982
3	-109.20000	78.38000	203.70000	1.137
2	71.05000	29.71000	100.00000	0.4904
2	-14.04000	8.44300	100.00000	0.3741
3	-11.81000	3.00900	200.00000	0.7107
2	-33.73000	-31.17000	101.30000	1.298
2	-31.71000	-28.02000	105.20000	1.28
2	-32.49000	-34.30000	108.00000	1.257
2	4.28200	-0.25360	101.80000	0.3734
"""
}


def get_tmp() -> str:
    """
    Get path to temporary (tmp) directory in operating system
    """
    if system() == "Darwin":
        return "/tmp"
    else:
        return gettempdir()


def make_sources(
        name_list: Union[str, Iterable[str]],
        data_list: Union[str, Iterable[str]]
):
    """
    Create temporary source files

    :param name: File's names with extension
    :param data: File's content
    """

    tmp = get_tmp()
    if isinstance(name_list, str) and isinstance(data_list, str):
        name_list = [name_list]
        data_list = [data_list]
    sources = [Path(tmp, name) for name in name_list]
    try:
        for source, data in zip(sources, data_list):
            source.write_text(data)
            yield source
    finally:
        for source in sources:
            source.unlink()


def make_mock_mingo():

    db = Database("mock_database", ask_to_create=False)

    names = [key for key in MOCK_SOURCE_DATA.keys()]
    data_list = [MOCK_SOURCE_DATA[key] for key in names]

    try:
        sourcegen = make_sources(names, data_list)
        for source in sourcegen:
            db.fill(source)
        yield db
    finally:
        db.drop()


@pytest.fixture()
def make_mock_source(request):
    """
    Create a temporary source file for testing.
    The content of the file is passed using the @pytest.mark.fixt_data
    decorator in the test function.
    """
    marker = request.node.get_closest_marker("fixt_data")
    if marker is None:
        raise ValueError("Missing content for mock source file")
    else:
        content = marker.args[0]
    tmp = get_tmp()
    source_file = Path(tmp, "mock_source.txt")
    source_file.write_text(content)
    yield source_file
    source_file.unlink()


@pytest.fixture()
def make_mock_database(monkeypatch):
    """
    Create temporary database for testing
    """
    monkeypatch.setattr("builtins.input", lambda _: "y")
    mock_db = Database("mock_database", True)
    yield mock_db
    mock_db.drop()
