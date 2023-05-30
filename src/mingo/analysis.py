from mingo import Database
from sqlalchemy import select, Double
from sqlalchemy.sql import func
import numpy as np
import matplotlib.pyplot as plt
from typing import Any
import pandas as pd


class Utils:

    def __init__(self) -> None:

        return None

    def subplot_organizer(self, items: int) -> tuple[int, int]:
        """
        Choose an adequate figure structure for the desired number of subplots

        :param items: Number of subplots to make
        :return: Number of rows and columns for the figure
        """

        match items:
            case 1:
                return (1, 1)
            case 2:
                return (1, 2)
            case 3:
                return (2, 2)
            case 4:
                return (2, 2)
            case 5:
                return (2, 3)
            case 6:
                return (2, 3)
            case _:
                raise ValueError("Too many items for a single figure")


class Hit_distribution(Utils):
    """
    Tools to analyze the relation between the energy of the primary cosmic
    ray and the number of hits per event
    """

    def __init__(self, db: Database) -> None:

        self.db = db
        self.dist_data: dict[str, dict[Any, Any]] = {}
        self.stats_data: dict[str, pd.DataFrame] = {}
        self.config = self.db.config.c
        self.plane = self.db.plane.c
        self.event = self.db.event.c
        self.hit = self.db.hit.c
        return None

    def plot_distribution(self):
        """
        Plot the distribution of the number of hits per event as a function
        of initial energy for each available detector configuration
        """

        rows, cols = self.subplot_organizer(len(self.dist_data.keys()))
        fig, axs = plt.subplots(ncols=cols, nrows=rows, figsize=(12, 6))

        for idx, (ax, key) in enumerate(zip(axs, self.dist_data)):

            for e, (hits, count) in self.dist_data[key].items():
                ax.plot(hits, count, label=f"{e:.0f} MeV")

            ax.set_xlabel("Number of hits per event")
            if idx == 0:
                ax.set_ylabel("Number of events")
            ax.set_title(key)
            ax.set_xlim(right=290)
            ax.set_ylim(top=375)
            ax.legend()

        fig.suptitle(
            "Distribution of hits per event as a function of initial energy")

        return fig

    def plot_stats(self):
        """
        Plot statistical properties of the distribution
        """

        rows, cols = self.subplot_organizer(len(self.dist_data.keys()))
        fig, axs = plt.subplots(ncols=cols, nrows=rows, figsize=(12, 6))

        for idx, (ax, key) in enumerate(zip(axs, self.stats_data)):
            data = self.stats_data[key]
            ax.plot(data["e_0"], data["avg"], label="Average")
            ax.fill_between(
                data["e_0"],
                data["avg"] - data["std"],
                data["avg"] + data["std"],
                label="Standard deviation", alpha=0.2
            )
            ax.scatter(data["e_0"], data["median"], label="Median", c="orange")
            ax.set_xlabel("Initial energy [MeV]")
            if idx == 0:
                ax.set_ylabel("Number of hits per event")
            ax.set_title(key)
            ax.set_ylim(top=175)
            ax.legend(loc="upper left")

        fig.suptitle(
            "Statistical properties of the distribution of hits per event"
        )

        return fig

    def stats(self, config_id: int, config_key: str) -> None:
        """
        Statistical properties of the distribution of the number of hits per
        event as a function of initial energy

        The result is saved as an item of the dictionary self.stats_data
        with key 'config_key'

        :param config_id: ID of existing detector configuration
        :param config_key: Name to identify given configuration
        """

        stmt = select(
            self.event.e_0,
            func.avg(self.event.n_hits, type_=Double),
            func.median(self.event.n_hits).over(self.event.e_0),
            func.stddev_samp(self.event.n_hits, type_=Double)
        )
        stmt = stmt.where(self.event.fk_config == config_id)
        stmt = stmt.group_by(self.event.e_0)

        with self.db.engine.connect() as conn:

            stats = pd.DataFrame(
                [
                    [e_0, avg, median, std]
                    for e_0, avg, median, std in conn.execute(stmt)
                ], columns=["e_0", "avg", "median", "std"]
            )

        stats["avg / std"] = stats["avg"] / stats["std"]

        self.stats_data[config_key] = stats.round(2)

        return None

    def distribution(self, config_id: int, config_key: str) -> None:
        """
        Distribution of the number of hits as a function of the initial energy
        of the event for each available value of initial energy

        The result is saved as an item of the dictionary self.dist_data
        with key 'config_key'

        :param config_id: ID of existing detector configuration
        :param config_key: Name to identify given configuration
        """
        with self.db.engine.connect() as conn:

            hits = {
                energy: np.array([
                    [hit, count] for hit, count in conn.execute(
                        select(self.event.n_hits, func.count(self.event.e_0))
                        .where(self.event.fk_config == config_id)
                        .where(self.event.e_0 == energy)
                        .group_by(self.event.n_hits)
                    )
                ]).swapaxes(0, 1) for energy, in conn.execute(
                    select(self.event.e_0)
                    .where(self.event.fk_config == config_id)
                    .distinct()
                    .order_by(self.event.e_0)
                )
            }

        self.dist_data[config_key] = hits

        return None

    def __call__(self, config_id: int, config_key: str) -> None:
        """
        Execute distribution and stats methods at once

        :param config_id: ID of existing detector configuration
        :param config_key: Name to identify given configuration
        """

        self.distribution(config_id, config_key)
        self.stats(config_id, config_key)

        return None
