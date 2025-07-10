package testutil

import (
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"
)

func AssertNotEqual(t testing.TB, expected, actual any, msgAndArgs ...any) {
	t.Helper()

	if reflect.DeepEqual(expected, actual) {
		t.Errorf(
			"Expected objects to be not equal, but they were:\nExpected: %v\nActual  : %v%s",
			expected,
			actual,
			formatMessage(msgAndArgs...),
		)
	}
}

func AssertLessThanEqual(t *testing.T, a, b uint64) {
	t.Helper()
	if a > b {
		t.Errorf("Expected %d <= %d", a, b)
	}
}

func AssertTrue(t testing.TB, condition bool, msgAndArgs ...any) {
	t.Helper()
	if !condition {
		t.Errorf("Expected condition to be true\n%s", FormatMsgAndArgs(msgAndArgs...))
	}
}

func AssertFalse(t testing.TB, condition bool, msgAndArgs ...any) {
	t.Helper()
	if condition {
		t.Errorf("Expected condition to be false\n%s", FormatMsgAndArgs(msgAndArgs...))
	}
}

func AssertEqual(t testing.TB, expected, actual any, msgAndArgs ...any) {
	t.Helper()
	if !reflect.DeepEqual(expected, actual) {
		t.Errorf(
			"Not equal: \nexpected: %v\nactual  : %v\n%s",
			expected,
			actual,
			FormatMsgAndArgs(msgAndArgs...),
		)
	}
}

func AssertNoError(t testing.TB, err error, msgAndArgs ...any) {
	t.Helper()
	if err != nil {
		t.Errorf("Unexpected error: %v\n%s", err, FormatMsgAndArgs(msgAndArgs...))
	}
}

func AssertError(t testing.TB, err error, msgAndArgs ...any) {
	t.Helper()
	if err == nil {
		t.Errorf("Expected an error but got nil\n%s", FormatMsgAndArgs(msgAndArgs...))
	}
}

func AssertErrorIs(t testing.TB, err, target error, msgAndArgs ...any) {
	t.Helper()
	if !errors.Is(err, target) {
		t.Errorf(
			"Expected error to be %v but got %v\n%s",
			target,
			err,
			FormatMsgAndArgs(msgAndArgs...),
		)
	}
}

func AssertLen(t testing.TB, object any, length int, msgAndArgs ...any) {
	t.Helper()
	v := reflect.ValueOf(object)
	if v.Len() != length {
		t.Errorf(
			"Length not equal: \nexpected: %d\nactual  : %d\n%s",
			length,
			v.Len(),
			FormatMsgAndArgs(msgAndArgs...),
		)
	}
}

func AssertEmpty(t testing.TB, object interface{}, msgAndArgs ...interface{}) {
	t.Helper()
	v := reflect.ValueOf(object)
	if v.Len() != 0 {
		t.Errorf("Expected empty but got length %d\n%s", v.Len(), FormatMsgAndArgs(msgAndArgs...))
	}
}

func AssertNil(t *testing.T, actual interface{}, msgAndArgs ...interface{}) {
	t.Helper()
	if !isNil(actual) {
		message := formatMessage(msgAndArgs...)
		// %#v provides a more detailed representation of the non-nil value.
		t.Fatalf("%s: Expected value to be nil, but was: %#v%s", getCallerInfo(), actual, message)
	}
}

func formatMessage(msgAndArgs ...interface{}) string {
	if len(msgAndArgs) == 0 || msgAndArgs[0] == nil {
		return ""
	}
	if len(msgAndArgs) == 1 {
		if msgStr, ok := msgAndArgs[0].(string); ok {
			return ": " + msgStr
		}
		return fmt.Sprintf(": %v", msgAndArgs[0])
	}
	if format, ok := msgAndArgs[0].(string); ok {
		return ": " + fmt.Sprintf(format, msgAndArgs[1:]...)
	}
	return fmt.Sprintf(": %v", msgAndArgs) // Fallback for non-string first arg
}

func isNil(value interface{}) bool {
	if value == nil {
		return true // Handles cases where value is explicitly nil
	}
	// Use reflect to check for typed nils (e.g., (*MyStruct)(nil))
	// which are not equal to `nil` itself but are still nil pointers/interfaces.
	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return rv.IsNil()
	default:
		// For other types (like struct, int, string), they cannot be "nil" in the same way.
		// An empty string or zero int is not nil. A zero struct is not nil.
		return false
	}
}

func getCallerInfo() string {
	// We want the caller of AssertNil/AssertNotNil, etc., so we skip:
	// 0: getCallerInfo (this function)
	// 1: AssertNil/AssertNotNil (the helper function)
	// 2: The actual test function that called the helper
	_, file, line, ok := runtime.Caller(2)
	if !ok {
		return ""
	}
	// Make file path relative to common project structures if possible,
	// e.g., strip GOROOT or GOPATH, or find a common project root.
	// For simplicity here, we'll just use a basic split.
	// You might want to make this more sophisticated based on your project structure.
	parts := strings.Split(file, "/")
	if len(parts) > 2 { // Show last two parts (e.g., directory/file.go)
		file = strings.Join(parts[len(parts)-2:], "/")
	} else if len(parts) > 0 {
		file = parts[len(parts)-1]
	}
	return fmt.Sprintf("%s:%d", file, line)
}

func AssertNotNil(t testing.TB, object any, msgAndArgs ...any) {
	t.Helper()
	if object == nil {
		t.Errorf("Expected not nil but got nil\n%s", FormatMsgAndArgs(msgAndArgs...))
		return
	}

	v := reflect.ValueOf(object)
	if v.Kind() == reflect.Ptr && v.IsNil() {
		t.Errorf("Expected not nil but got nil pointer\n%s", FormatMsgAndArgs(msgAndArgs...))
	}
}

// Format message and arguments for error output
func FormatMsgAndArgs(msgAndArgs ...any) string {
	if len(msgAndArgs) == 0 {
		return ""
	}
	if len(msgAndArgs) == 1 {
		return fmt.Sprintf("\nMessage: %s", msgAndArgs[0])
	}
	return fmt.Sprintf("\nMessage: %s", fmt.Sprintf(msgAndArgs[0].(string), msgAndArgs[1:]...))
}

// Custom require helpers that fail the test immediately
func RequireNoError(t testing.TB, err error, msgAndArgs ...any) {
	t.Helper()
	if err != nil {
		t.Fatalf("Required no error but got: %v\n%s", err, FormatMsgAndArgs(msgAndArgs...))
	}
}

func RequireNotNil(t testing.TB, object any, msgAndArgs ...any) {
	t.Helper()
	if object == nil {
		t.Fatalf("Required not nil but got nil\n%s", FormatMsgAndArgs(msgAndArgs...))
		return
	}

	v := reflect.ValueOf(object)
	if v.Kind() == reflect.Ptr && v.IsNil() {
		t.Fatalf("Required not nil but got nil pointer\n%s", FormatMsgAndArgs(msgAndArgs...))
	}
}

func AssertContains(t testing.TB, s, substr string, msgAndArgs ...any) {
	t.Helper()
	if !strings.Contains(s, substr) {
		t.Errorf(
			"Expected string to contain substring:\nstring: %q\nsubstring: %q\n%s",
			s,
			substr,
			FormatMsgAndArgs(msgAndArgs...),
		)
	}
}

// AssertFileRemoved checks if the given path exists in the slice of removed file paths.
func AssertFileRemoved(t testing.TB, path string, removedFiles []string, msgAndArgs ...any) {
	t.Helper()
	found := slices.Contains(removedFiles, path)
	if !found {
		t.Errorf(
			"Expected file %q to be removed, but it wasn't in the list of removed files: %v\n%s",
			path,
			removedFiles,
			FormatMsgAndArgs(msgAndArgs...),
		)
	}
}

// AssertPositive checks if a uint64 value is greater than zero.
func AssertPositive(t testing.TB, val uint64, msgAndArgs ...any) {
	t.Helper()
	if val == 0 {
		t.Errorf("Expected value > 0, got 0\n%s", FormatMsgAndArgs(msgAndArgs...))
	}
}
