#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Validate that tagged flows stay separated from other enabled flows."""

import argparse

from landingzones.transfer_definitions import (
    normalize_tags,
    normalize_tags_text,
    normalize_transfer_path,
)
from landingzones.transfer_loading import (
    filter_transfers_by_tags,
    load_reporting_transfers,
)


def _row_context(row):
    """Return a compact transfer context label."""
    return "{0} [{1}@{2}]".format(
        row["identifiers"],
        row["users"],
        row["system"],
    )


def _path_contains(parent_path, child_path):
    """Return True when one normalized endpoint contains the other."""
    if not parent_path or not child_path:
        return False
    return parent_path == child_path or child_path.startswith(parent_path + "/")


def detect_source_overlap_collisions(tagged_df, other_df):
    """Return collisions where a tagged source overlaps another source."""
    findings = []
    for _, tagged in tagged_df.iterrows():
        tagged_source = normalize_transfer_path(tagged["source"], strip_wildcard=True)
        for _, other in other_df.iterrows():
            if tagged["system"] != other["system"]:
                continue
            other_source = normalize_transfer_path(other["source"], strip_wildcard=True)
            if not tagged_source or not other_source:
                continue
            if not (
                _path_contains(tagged_source, other_source)
                or _path_contains(other_source, tagged_source)
            ):
                continue
            findings.append(
                {
                    "type": "source_overlap",
                    "tagged_identifier": tagged["identifiers"],
                    "tagged_tags": normalize_tags_text(tagged.get("tags", "")),
                    "tagged_context": _row_context(tagged),
                    "other_identifier": other["identifiers"],
                    "other_context": _row_context(other),
                    "tagged_path": tagged_source,
                    "other_path": other_source,
                }
            )
    return findings


def detect_destination_handoff_collisions(tagged_df, other_df):
    """Return collisions where a tagged destination feeds another source."""
    findings = []
    for _, tagged in tagged_df.iterrows():
        tagged_destination = normalize_transfer_path(
            tagged["destination"],
            strip_wildcard=False,
        )
        for _, other in other_df.iterrows():
            other_source = normalize_transfer_path(other["source"], strip_wildcard=True)
            if not tagged_destination or not other_source:
                continue
            if tagged_destination != other_source:
                continue
            findings.append(
                {
                    "type": "destination_handoff",
                    "tagged_identifier": tagged["identifiers"],
                    "tagged_tags": normalize_tags_text(tagged.get("tags", "")),
                    "tagged_context": _row_context(tagged),
                    "other_identifier": other["identifiers"],
                    "other_context": _row_context(other),
                    "tagged_path": tagged_destination,
                    "other_path": other_source,
                }
            )
    return findings


def detect_separation_collisions(transfers_df, requested_tags):
    """Return tagged/other subsets plus all detected path interaction findings."""
    if requested_tags:
        tagged_df = filter_transfers_by_tags(transfers_df, requested_tags)
    else:
        tagged_df = transfers_df[
            transfers_df["tags"].apply(lambda value: bool(normalize_tags(value)))
        ].copy()
    if tagged_df.empty:
        return tagged_df, transfers_df.iloc[0:0].copy(), []

    other_df = transfers_df[
        ~transfers_df["identifiers"].isin(tagged_df["identifiers"])
    ].copy()
    findings = []
    findings.extend(detect_source_overlap_collisions(tagged_df, other_df))
    findings.extend(detect_destination_handoff_collisions(tagged_df, other_df))
    return tagged_df, other_df, findings


def _group_findings(findings):
    """Group findings by collision type."""
    grouped = {}
    for finding in findings:
        grouped.setdefault(finding["type"], []).append(finding)
    return grouped


def print_separation_report(requested_tags, tagged_df, other_df, findings):
    """Print a grouped operator-readable validation report."""
    tag_text = normalize_tags_text(requested_tags) or "any"
    if tagged_df.empty:
        print("No matching tagged transfers found for tags: {0}".format(tag_text))
        return

    print(
        "Separation check for tags: {0} ({1} tagged transfer(s) vs {2} other enabled transfer(s))".format(
            tag_text,
            len(tagged_df),
            len(other_df),
        )
    )
    if not findings:
        print("No collisions found.")
        return

    print("Found {0} collision(s).".format(len(findings)))
    grouped = _group_findings(findings)
    for collision_type in ("source_overlap", "destination_handoff"):
        type_findings = grouped.get(collision_type, [])
        if not type_findings:
            continue
        print("")
        if collision_type == "source_overlap":
            print("Source Overlap Collisions ({0})".format(len(type_findings)))
        else:
            print("Destination Handoff Collisions ({0})".format(len(type_findings)))
        for finding in type_findings:
            print(
                "- tagged: {0} tags={1}".format(
                    finding["tagged_context"],
                    finding["tagged_tags"] or "(none)",
                )
            )
            print("  other:  {0}".format(finding["other_context"]))
            if collision_type == "source_overlap":
                print("  tagged source: {0}".format(finding["tagged_path"]))
                print("  other source:  {0}".format(finding["other_path"]))
            else:
                print("  tagged destination: {0}".format(finding["tagged_path"]))
                print("  other source:       {0}".format(finding["other_path"]))


def build_parser():
    """Build the separation validation CLI parser."""
    parser = argparse.ArgumentParser(
        description="Validate that tagged transfers stay isolated from other enabled flows",
    )
    parser.add_argument("--config", "-c", default=None)
    parser.add_argument("--transfers", "-t", default=None)
    parser.add_argument(
        "--tag",
        action="append",
        default=[],
        help=(
            "Tag to select transfers for the separation check; repeatable. "
            "Defaults to any tagged transfer."
        ),
    )
    return parser


def main(argv=None):
    """CLI entrypoint."""
    parser = build_parser()
    args = parser.parse_args(argv)
    transfers_df = load_reporting_transfers(
        config_file=args.config,
        transfers_file=args.transfers,
    )
    tagged_df, other_df, findings = detect_separation_collisions(
        transfers_df,
        args.tag,
    )
    print_separation_report(args.tag, tagged_df, other_df, findings)
    return 1 if findings else 0


if __name__ == "__main__":
    raise SystemExit(main())
