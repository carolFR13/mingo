import os
from pathlib import Path
import platform
import tempfile
from typing import Union

HEADER = """HEADER
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
DATA"""


def cm2mm(value: str) -> str:
    """
    Convert cm into mm with five decimal places

    :param value: Length in cm.
    :return str: Length in mm.
    """

    return f"{float(value) * 10:+.4e}"


def reformat(
        file: Path,
        e_max: Union[None, int] = None,
        e_min: Union[None, int] = None,
        p2_thickness: Union[None, float] = None,
        p4_thickness: Union[None, float] = None
) -> None:
    """
    Reformat original source file.

    The original file is modified to adjust to the new header format.

    :param file: Local or global path to source file.
    :param e_max: Maximum value of initial energy in MeV.
    :param e_min: Minimum value of initial energy in MeV.
    :param p2_thickness: Thickness of the second absorption plane in mm.
    :param p4_thickness: Thickness of the fourth absorption plane in mm.
    :return None:
    """
    particle = 1
    n_events = 10000
    e_dist = 0
    theta_min = 0
    theta_max = 0
    x_detector_plane = 999
    y_detector_plane = 999
    z_detector_plane = 22

    # Create temporary formatted file
    if platform.system() == "Darwin":
        tmp = "/tmp"
    else:
        tmp = tempfile.gettempdir()
    tmp_source = Path(tmp, "tmp_" + file.name)

    with open(tmp_source, "w") as out:

        # Add header
        out.write(HEADER + "\n")

        # Add CASE data
        if e_max is None:
            e_max = int(input("Emax [MeV]: "))
        if e_min is None:
            e_min = int(input("Emin [MeV]: "))
        case_data = (
            f"{particle}\t{n_events}\t{e_min}\t{e_max}\t{e_dist}\t{theta_min}"
            f"\t{theta_max}\t{x_detector_plane}\t{y_detector_plane}\t"
            f"{z_detector_plane}\n"
        )
        out.write(case_data)

        # Add ACTIVE PLANES data
        out.write("0\t100\t200\t400\n")

        # Add PASSIVE PLANES data
        if p2_thickness is None:
            p2_thickness = float(input("Abs plane 2 - thickness [mm]: "))
        if p4_thickness is None:
            p4_thickness = float(input("Abs plane 4 - thickness [mm]: "))
        passive_plane_data = (
            f"null\t22\tnull\t222\tnull\t{p2_thickness}\tnull\t{p4_thickness}"
            f"\tnull\t0\tnull\t0\n"
        )
        out.write(passive_plane_data)

        # Add EVENTs and HITs
        with open(file, "r") as source:
            for _ in range(3):
                source.readline()
            if "HEADER" in source.readline():
                for line in source:
                    data = line[:-1].split("\t")
                    if len(data) == 8:
                        out.write(
                            f"{data[0]}\t{data[1]}\t{cm2mm(data[2])}\t"
                            f"{cm2mm(data[3])}\t{cm2mm(data[4])}\t"
                            f"{data[5]}\t{data[6]}\t{data[7]}\n"
                        )
                    elif len(data) == 5:
                        out.write(
                            f"{data[0]}\t{cm2mm(data[1])}\t"
                            f"{cm2mm(data[2])}\t{cm2mm(data[3])}\t"
                            f"{data[4]}\n"
                        )
                    else:
                        raise ValueError(f"Unexpected length {data}")
            else:
                os.remove(tmp_source)
                raise ValueError("Unexpected format in source file")

    tmp_source.rename(file)

    return None
