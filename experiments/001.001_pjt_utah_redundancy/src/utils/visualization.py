import matplotlib.pyplot as plt
import contextily as cx

from ..core.utils import extract_detour_geodataframes_for_edge_id

def render_detour_map(
    out_fpath=None,
    edge_gdf=None,
    origin_point_gdf=None,
    destination_point_gdf=None,
    base_path_edges_gdf=None,
    detour_path_edges_gdf=None,
    viz_plot=None,
    show_plot=True
):
    nonnone_gdfs = [d for d in [
        edge_gdf,
        origin_point_gdf,
        destination_point_gdf,
        base_path_edges_gdf,
        detour_path_edges_gdf
    ] if d is not None]

    if not nonnone_gdfs:
        raise BaseException('Nothing to render.')

    map_crs = nonnone_gdfs[0].crs

    if viz_plot and not show_plot:
        raise BaseException('INVARIANT BROKEN: If viz_plot passed in, the caller must call plt.close(f).')

    render_either_path = base_path_edges_gdf is not None or detour_path_edges_gdf is not None
    focused_edge_color = 'red' if render_either_path else 'black'

    f, ax = viz_plot or plt.subplots(
        figsize=(10,10),
        constrained_layout=True
    )

    total_bounds = [
        getattr(edge_gdf, 'total_bounds', None),
        getattr(origin_point_gdf, 'total_bounds', None),
        getattr(destination_point_gdf, 'total_bounds', None),
        getattr(base_path_edges_gdf, 'total_bounds', None),
        getattr(detour_path_edges_gdf, 'total_bounds', None)
    ]

    xmin = min(b[0] for b in total_bounds if b is not None)
    xmax = max(b[2] for b in total_bounds if b is not None)

    ymin = min(b[1] for b in total_bounds if b is not None)
    ymax = max(b[3] for b in total_bounds if b is not None)

    threshold = 0.005
    ax.set_xlim(xmin - threshold, xmax + threshold)
    ax.set_ylim(ymin - threshold, ymax + threshold)

    if edge_gdf is not None:
        edge_gdf.plot(
            ax=ax,
            color=focused_edge_color,
            linewidth=6,
            aspect=1,
        )

    if base_path_edges_gdf is not None:
        base_path_edges_gdf.plot(
            ax=ax,
            color='green',
            linewidth=3,
            aspect=1
        )

    if detour_path_edges_gdf is not None:
        detour_path_edges_gdf.plot(
            ax=ax,
            color='black',
            linewidth=3,
            aspect=1
        )

    cx.add_basemap(
        ax,
        crs=map_crs,
        source=cx.providers.Esri.WorldShadedRelief,
        attribution=' ' * 60 + 'Hillshade Tiles © Esri — Source: Esri',
        alpha=0.8
    )
    cx.add_basemap(
        ax,
        crs=map_crs,
        source=cx.providers.OpenStreetMap.Mapnik,
        alpha=0.4
    )

    ax.set_axis_off()

    if origin_point_gdf is not None:
        origin_point_gdf.plot(
            ax=ax,
            color='green',
            markersize=15,
            aspect=1
        )

        ax.annotate(
            'Origin',
            xy=(
                origin_point_gdf.iloc[0].geometry.x,
                origin_point_gdf.iloc[0].geometry.y,
            ),
            color='green',
            fontweight='heavy',
            ha='left',
            va='bottom',
            # in_layout=True
        )

    if destination_point_gdf is not None:
        destination_point_gdf.plot(
            ax=ax,
            color='green',
            markersize=15,
            aspect=1
        )

        ax.annotate(
            'Destination',
            xy=(
                destination_point_gdf.iloc[0].geometry.x,
                destination_point_gdf.iloc[0].geometry.y,
            ),
            color='green',
            fontweight='heavy',
            ha='right',
            va='bottom',
            # in_layout=True
        )

    if out_fpath:
        f.savefig(
            fname=out_fpath,
            bbox_inches='tight',
            pad_inches=0,
            transparent=True,
        )

    # If f was not passed in, and show_plot is False, then do not render the plot.
    # If f was passed in, it the responsibility of the caller
    if not any([viz_plot, show_plot]):
        plt.close(f)


# NOTE: With sufficient refactoring, this function could use output_detour_map for each map.
def render_side_by_side_detour_map(
    edge_detour_info=None,
    edge_gdf=None,
    origin_point_gdf=None,
    destination_point_gdf=False,
    base_path_edges_gdf=False,
    detour_path_edges_gdf=False,
    out_fpath=None,
    show_plot=True
):
    f, axes = plt.subplots(
        ncols=2,
        figsize=(15,15),
        constrained_layout=True
    )

    render_detour_map(
        edge_gdf=edge_gdf,
        origin_point_gdf=origin_point_gdf,
        destination_point_gdf=destination_point_gdf,
        base_path_edges_gdf=base_path_edges_gdf,
        detour_path_edges_gdf=None,
        viz_plot=(f, axes[0]),
        out_fpath=None,
    )

    render_detour_map(
        out_fpath=None,
        edge_gdf=edge_gdf,
        origin_point_gdf=origin_point_gdf,
        destination_point_gdf=destination_point_gdf,
        base_path_edges_gdf=None,
        detour_path_edges_gdf=detour_path_edges_gdf,
        viz_plot=(f, axes[1]),
    )

    axes[0].set_title(
        'No Detour',
        y=1.0,
        pad=-4,
        color='black',
        fontweight='heavy',
        backgroundcolor='grey'
    )

    axes[0].set_title(
        f'time: {edge_detour_info["base_travel_time_sec"]} sec\ndist:{edge_detour_info["base_length_miles"]} mi',
        y=0,
        pad=-26,
        color='black',
        fontweight='heavy',
        backgroundcolor='grey',
        loc='right',
        wrap=False,
        # horizontalalignment='left'
    )

    axes[1].set_title(
        'With Detour',
        y=1.0,
        pad=-4,
        color='black',
        fontweight='heavy',
        backgroundcolor='grey'
    )

    axes[1].set_title(
        f'time: {edge_detour_info["detour_travel_time_sec"]} sec\ndist:{edge_detour_info["detour_length_miles"]} mi',
        y=0,
        pad=-26,
        color='black',
        fontweight='heavy',
        backgroundcolor='grey',
        loc='right',
        wrap=False,
        # horizontalalignment='left'
    )

    if out_fpath:
        f.savefig(
            fname=out_fpath,
            bbox_inches='tight',
            pad_inches=0,
            transparent=True,
        )

    if not show_plot:
        plt.close(f)

def render_map_for_edge(
    roadways_gdf,
    edge_id,
    out_fpath=None,
    show_plot=True
):
    return render_detour_map(
        out_fpath=out_fpath,
        edge_gdf=roadways_gdf.loc[[edge_id], 'geometry'],
        show_plot=show_plot
    )

# NOTE: With sufficient refactoring, this function could use output_detour_map for each map.
def render_side_by_side_detour_map_for_edge(
    roadways_gdf,
    detour_info_df,
    edge_id,
    out_fpath=None,
    show_plot=True
):
    detour_dfs = extract_detour_geodataframes_for_edge_id(
        roadways_gdf=roadways_gdf,
        detour_info_df=detour_info_df,
        edge_id=edge_id
    )

    return render_side_by_side_detour_map(
        out_fpath=out_fpath,
        show_plot=show_plot,
        **detour_dfs,
    )