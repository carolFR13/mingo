import os

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

    return f"{float(value) * 10:.5f}"


def format_source(original: str, formatted: str) -> None:
    """
    Reformat source file to fit new header
    """
    particle = 1
    n_events = 10000
    e_dist = 0
    theta_min = 0
    theta_max = 0
    x_detector_plane = 999
    y_detector_plane = 999
    z_detector_plane = 22

    with open(formatted, "w") as out:

        # Add header
        out.write(HEADER + "\n")

        # Add CASE data
        e_max = int(input("Emax [MeV]: "))
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
        p2_thickness = float(input("Abs plane 2 - thickness [mm]: "))
        p4_thickness = float(input("Abs plane 4 - thickness [mm]: "))
        passive_plane_data = (
            f"null\t22\tnull\t222\tnull\t{p2_thickness}\tnull\t{p4_thickness}"
            f"\tnull\t0\tnull\t0\n"
        )
        out.write(passive_plane_data)

        # Add EVENTs and HITs
        with open(original, "r") as source:
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
                os.remove(formatted)
                raise ValueError("Unexpected format in source file")

    return None
