"""
Lead Importer - CSV import with validation and deduplication.

Designed to work with exports from common enrichment tools:
- Apollo.io
- Clay
- LinkedIn Sales Navigator
- ZoomInfo
- Clearbit

Usage:
    from leads import LeadImporter

    importer = LeadImporter()
    result = importer.import_csv("leads.csv")

    print(f"Imported: {result.imported}")
    print(f"Skipped duplicates: {result.duplicates}")
    print(f"Invalid rows: {result.invalid}")
"""

import csv
import re
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Set, Tuple
from pathlib import Path
import sys

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from outbound_engine import Prospect


class ValidationError(Exception):
    """Raised when lead validation fails."""
    pass


@dataclass
class ImportResult:
    """Results from a lead import operation."""
    imported: int = 0
    duplicates: int = 0
    invalid: int = 0
    prospects: List[Prospect] = field(default_factory=list)
    errors: List[Dict] = field(default_factory=list)

    @property
    def total_processed(self) -> int:
        return self.imported + self.duplicates + self.invalid

    def summary(self) -> str:
        return (
            f"Import complete: {self.imported} imported, "
            f"{self.duplicates} duplicates skipped, "
            f"{self.invalid} invalid rows"
        )


# Common column name mappings from various export tools
COLUMN_MAPPINGS = {
    # Email variations
    "email": "email",
    "email_address": "email",
    "work_email": "email",
    "contact_email": "email",
    "primary_email": "email",

    # First name variations
    "first_name": "first_name",
    "firstname": "first_name",
    "first": "first_name",
    "given_name": "first_name",

    # Last name variations
    "last_name": "last_name",
    "lastname": "last_name",
    "last": "last_name",
    "surname": "last_name",
    "family_name": "last_name",

    # Company variations
    "company": "company",
    "company_name": "company",
    "organization": "company",
    "org": "company",
    "account_name": "company",

    # Title variations
    "title": "title",
    "job_title": "title",
    "position": "title",
    "role": "title",

    # Industry variations
    "industry": "industry",
    "company_industry": "industry",
    "sector": "industry",

    # Company size variations
    "company_size": "company_size",
    "employees": "company_size",
    "employee_count": "company_size",
    "headcount": "company_size",
    "size": "company_size",

    # LinkedIn variations
    "linkedin_url": "linkedin_url",
    "linkedin": "linkedin_url",
    "linkedin_profile": "linkedin_url",
    "person_linkedin_url": "linkedin_url",
}


class LeadImporter:
    """
    Imports and validates leads from CSV files.

    Features:
    - Auto-detects column mappings from common export formats
    - Validates email format
    - Deduplicates by email
    - Tracks invalid rows with reasons
    - Supports custom field mapping
    """

    # Regex for basic email validation
    EMAIL_REGEX = re.compile(
        r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    )

    # Common invalid/test email patterns to filter
    INVALID_EMAIL_PATTERNS = [
        r".*@example\.com$",
        r".*@test\.com$",
        r".*@mailinator\.com$",
        r"^test@.*",
        r"^noreply@.*",
        r"^no-reply@.*",
        r"^info@.*",
        r"^contact@.*",
        r"^sales@.*",
        r"^support@.*",
    ]

    def __init__(
        self,
        custom_mappings: Optional[Dict[str, str]] = None,
        skip_generic_emails: bool = True
    ):
        """
        Initialize the importer.

        Args:
            custom_mappings: Additional column name mappings
            skip_generic_emails: Whether to skip info@, sales@, etc.
        """
        self.column_mappings = {**COLUMN_MAPPINGS}
        if custom_mappings:
            self.column_mappings.update(custom_mappings)

        self.skip_generic_emails = skip_generic_emails
        self._seen_emails: Set[str] = set()

    def import_csv(
        self,
        filepath: str,
        encoding: str = "utf-8-sig",  # Handles BOM from Excel exports
        delimiter: str = ","
    ) -> ImportResult:
        """
        Import leads from a CSV file.

        Args:
            filepath: Path to CSV file
            encoding: File encoding (default handles Excel exports)
            delimiter: CSV delimiter

        Returns:
            ImportResult with imported prospects and stats
        """
        result = ImportResult()
        self._seen_emails.clear()

        filepath = Path(filepath)
        if not filepath.exists():
            raise FileNotFoundError(f"CSV file not found: {filepath}")

        with open(filepath, "r", encoding=encoding) as f:
            reader = csv.DictReader(f, delimiter=delimiter)

            # Normalize headers
            if reader.fieldnames is None:
                raise ValidationError("CSV file has no headers")

            header_map = self._map_headers(reader.fieldnames)

            for row_num, row in enumerate(reader, start=2):  # Start at 2 (header is 1)
                try:
                    prospect = self._process_row(row, header_map, row_num)

                    if prospect is None:
                        # Duplicate
                        result.duplicates += 1
                        continue

                    result.prospects.append(prospect)
                    result.imported += 1

                except ValidationError as e:
                    result.invalid += 1
                    result.errors.append({
                        "row": row_num,
                        "error": str(e),
                        "data": dict(row)
                    })

        return result

    def _map_headers(self, headers: List[str]) -> Dict[str, str]:
        """Map CSV headers to standard field names."""
        header_map = {}

        for header in headers:
            normalized = header.lower().strip().replace(" ", "_")

            if normalized in self.column_mappings:
                header_map[header] = self.column_mappings[normalized]
            else:
                # Keep as custom field
                header_map[header] = f"custom_{normalized}"

        return header_map

    def _process_row(
        self,
        row: Dict[str, str],
        header_map: Dict[str, str],
        row_num: int
    ) -> Optional[Prospect]:
        """
        Process a single CSV row into a Prospect.

        Returns None if duplicate, raises ValidationError if invalid.
        """
        # Map row to standard fields
        mapped = {}
        custom_fields = {}

        for original_header, value in row.items():
            if original_header not in header_map:
                continue

            field_name = header_map[original_header]
            clean_value = value.strip() if value else ""

            if field_name.startswith("custom_"):
                if clean_value:
                    custom_fields[field_name.replace("custom_", "")] = clean_value
            else:
                mapped[field_name] = clean_value

        # Validate required fields
        email = mapped.get("email", "").lower()

        if not email:
            raise ValidationError("Missing email address")

        if not self._validate_email(email):
            raise ValidationError(f"Invalid email format: {email}")

        if self.skip_generic_emails and self._is_generic_email(email):
            raise ValidationError(f"Generic email skipped: {email}")

        # Check for duplicate
        if email in self._seen_emails:
            return None

        self._seen_emails.add(email)

        # Validate other required fields
        first_name = mapped.get("first_name", "")
        last_name = mapped.get("last_name", "")
        company = mapped.get("company", "")

        if not first_name:
            raise ValidationError("Missing first name")

        if not last_name:
            raise ValidationError("Missing last name")

        if not company:
            raise ValidationError("Missing company name")

        # Build Prospect
        return Prospect(
            email=email,
            first_name=first_name,
            last_name=last_name,
            company=company,
            title=mapped.get("title"),
            industry=mapped.get("industry"),
            company_size=mapped.get("company_size"),
            linkedin_url=mapped.get("linkedin_url"),
            custom_fields=custom_fields if custom_fields else None
        )

    def _validate_email(self, email: str) -> bool:
        """Check if email has valid format."""
        return bool(self.EMAIL_REGEX.match(email))

    def _is_generic_email(self, email: str) -> bool:
        """Check if email is a generic role-based address."""
        for pattern in self.INVALID_EMAIL_PATTERNS:
            if re.match(pattern, email, re.IGNORECASE):
                return True
        return False

    def validate_file(self, filepath: str) -> Tuple[bool, List[str]]:
        """
        Validate a CSV file without importing.

        Returns:
            Tuple of (is_valid, list of issues)
        """
        issues = []
        filepath = Path(filepath)

        if not filepath.exists():
            return False, ["File not found"]

        try:
            with open(filepath, "r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)

                if not reader.fieldnames:
                    return False, ["No headers found"]

                header_map = self._map_headers(reader.fieldnames)

                # Check for required columns
                mapped_fields = set(header_map.values())
                required = {"email", "first_name", "last_name", "company"}
                missing = required - mapped_fields

                if missing:
                    issues.append(f"Missing required columns: {missing}")

                # Check first few rows
                for i, row in enumerate(reader):
                    if i >= 5:  # Only check first 5 rows
                        break

                    email = row.get(
                        next((k for k, v in header_map.items() if v == "email"), ""),
                        ""
                    )
                    if email and not self._validate_email(email.strip().lower()):
                        issues.append(f"Row {i+2}: Invalid email format")

        except Exception as e:
            return False, [f"Error reading file: {str(e)}"]

        return len(issues) == 0, issues


def import_from_apollo(filepath: str) -> ImportResult:
    """
    Convenience function for Apollo.io exports.

    Apollo exports typically have columns:
    - Email, First Name, Last Name, Company, Title, Industry, # Employees
    """
    importer = LeadImporter(custom_mappings={
        "# employees": "company_size",
        "person_linkedin_url": "linkedin_url",
    })
    return importer.import_csv(filepath)


def import_from_clay(filepath: str) -> ImportResult:
    """
    Convenience function for Clay exports.

    Clay exports can vary but typically include:
    - email, firstName, lastName, companyName, title, industry
    """
    importer = LeadImporter(custom_mappings={
        "companyname": "company",
        "firstname": "first_name",
        "lastname": "last_name",
    })
    return importer.import_csv(filepath)


def import_from_linkedin(filepath: str) -> ImportResult:
    """
    Convenience function for LinkedIn Sales Navigator exports.

    LinkedIn exports typically have:
    - Email, First Name, Last Name, Company, Title
    """
    importer = LeadImporter()
    return importer.import_csv(filepath)


# CLI interface
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Import leads from CSV")
    parser.add_argument("filepath", help="Path to CSV file")
    parser.add_argument("--validate-only", action="store_true", help="Only validate, don't import")
    parser.add_argument("--source", choices=["apollo", "clay", "linkedin", "generic"], default="generic")

    args = parser.parse_args()

    if args.validate_only:
        importer = LeadImporter()
        is_valid, issues = importer.validate_file(args.filepath)

        if is_valid:
            print("File is valid")
        else:
            print("Validation issues:")
            for issue in issues:
                print(f"  - {issue}")
    else:
        if args.source == "apollo":
            result = import_from_apollo(args.filepath)
        elif args.source == "clay":
            result = import_from_clay(args.filepath)
        elif args.source == "linkedin":
            result = import_from_linkedin(args.filepath)
        else:
            importer = LeadImporter()
            result = importer.import_csv(args.filepath)

        print(result.summary())

        if result.errors:
            print(f"\nFirst 5 errors:")
            for error in result.errors[:5]:
                print(f"  Row {error['row']}: {error['error']}")
