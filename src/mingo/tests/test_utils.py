from mingo import reformat
import pytest
from pathlib import Path
from mock_data import make_mock_source

MOCK_SOURCE = """----- FILE HEADER ----------------------------------
Event_Number	Ini_E[MeV]	Ini_X[cm]	Ini_Y[cm]	Ini_Z[cm]	Ini_Theta	Ini_Phi	Number_of_hits
Plane_h	X_h[cm]	Y_h[cm]	Z_h[cm]	Time_h[ns]	 Being 'h' each hit of the event
----- END OF HEADER ----------------------------------
1	800	0	0	0	0	0	24
1	0	0	0	0
2	-0.07172	-0.9398	10	0.3692
3	-1.478	-2.662	20	0.7113
4	-12.21	4.655	42.02	1.589
3	4.782	-1.086	21.69	0.901
4	-10.07	-18.65	41.95	1.767
4	-4.43	-32.29	40	2.005
2	-0.9686	-0.2483	10	0.3695
3	-3.241	3.25	20	0.7339
3	4.556e-05	2.288	20.05	0.8806
2	-0.09783	-0.74	10	0.3683
3	-0.9577	-2.49	20	0.7084
3	-0.9664	-2.509	20.11	0.712
2	-0.1496	0.05536	10	0.367
3	0.08875	0.6635	20	0.7014
3	-0.4356	0.1738	22.11	0.7711
3	-0.4356	0.1738	22.11	0.7711
3	-0.4356	0.1738	22.11	0.7711
4	10.28	-15.49	40	1.65
2	20.67	-3.364	11.23	1.626
3	13.52	-1.192	20.93	1.218
3	0.0002592	0.2351	20.64	0.7218
2	6.831	-2.156	10	0.4779
2	2.383	0.395	10	0.3809
"""                                 # noqa: E501

EXPECTED_HEADER = """HEADER
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
DATA\n"""

EXPECTED_CASE = "1\t10000\t800\t800\t0\t0\t0\t999\t999\t22\n"

EXPECTED_ACTIVE_PLANES = "0\t100\t200\t400\n"

EXPECTED_PASSIVE_PLANES = (
    "null\t22\tnull\t222\tnull\t16.2\tnull\t10.4\tnull\t0\tnull\t0\n"
)

EXPECTED_EVENT = "1\t800\t+0.0000e+00\t+0.0000e+00\t+0.0000e+00\t0\t0\t24\n"

EXPECTED_HIT = "3\t-3.2410e+01\t+3.2500e+01\t+2.0000e+02\t0.7339\n"


@pytest.mark.fixt_data(MOCK_SOURCE)
def test_reformat(monkeypatch, make_mock_source: Path) -> None:
    """
    Ensure that original source files are properly reformatted
    """

    source = make_mock_source

    # Setup mock values for interactive input
    user_inputs = iter(["800", "800", "16.2", "10.4"])

    def mock_input(_):
        return next(user_inputs)
    monkeypatch.setattr("builtins.input", mock_input)

    # Reformat mock source file
    reformat(source)

    with open(source, "r") as file:

        # Check header
        result = file.readline()
        for _ in range(45):
            result += file.readline()
        assert result == EXPECTED_HEADER

        # Check CASE data
        assert file.readline() == EXPECTED_CASE

        # Check ACTIVE PLANES data
        assert file.readline() == EXPECTED_ACTIVE_PLANES

        # Check PASSIVE PLANES data
        assert file.readline() == EXPECTED_PASSIVE_PLANES

        # Check EVENT
        assert file.readline() == EXPECTED_EVENT

        # Check HIT
        for _ in range(8):
            file.readline()
        assert file.readline() == EXPECTED_HIT

    return None
