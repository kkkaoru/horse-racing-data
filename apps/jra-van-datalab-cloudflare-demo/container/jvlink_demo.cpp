#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <oleauto.h>

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cwchar>
#include <string>
#include <string_view>
#include <vector>

namespace {

constexpr CLSID kJvLinkClsid = {
    0x2AB1774D,
    0x0C41,
    0x11D7,
    {0x91, 0x6F, 0x00, 0x03, 0x47, 0x9B, 0xEB, 0x3F},
};
constexpr DISPID kSetSavePath = 0x1;
constexpr DISPID kInit = 0x4;
constexpr DISPID kClose = 0x5;
constexpr DISPID kOpen = 0x7;
constexpr DISPID kStatus = 0x8;
constexpr DISPID kCancel = 0xB;
constexpr DISPID kSetServiceKey = 0xD;
constexpr DISPID kSetSaveFlag = 0xF;
constexpr DISPID kGets = 0x16;
constexpr LONG kAlreadyRegistered = -101;
constexpr UINT kCp932 = 932;
constexpr LONG kBufferSize = 110000;

struct Config {
  std::wstring service_key;
  std::wstring data_spec = L"RACE";
  std::wstring from_time;
  std::wstring output;
  std::wstring save_path = L"C:\\JVData";
  LONG limit = 20;
  ULONGLONG timeout_ms = 300000;
};

class ComApartment {
 public:
  ComApartment() : result_(CoInitializeEx(nullptr, COINIT_APARTMENTTHREADED)) {}
  ~ComApartment() {
    if (SUCCEEDED(result_)) {
      CoUninitialize();
    }
  }
  ComApartment(const ComApartment&) = delete;
  ComApartment& operator=(const ComApartment&) = delete;
  [[nodiscard]] HRESULT result() const { return result_; }

 private:
  HRESULT result_;
};

class ScopedVariant {
 public:
  ScopedVariant() { VariantInit(&value_); }
  ~ScopedVariant() { VariantClear(&value_); }
  ScopedVariant(const ScopedVariant&) = delete;
  ScopedVariant& operator=(const ScopedVariant&) = delete;
  VARIANT* get() { return &value_; }
  VARIANT& value() { return value_; }

 private:
  VARIANT value_;
};

class ScopedBstr {
 public:
  explicit ScopedBstr(std::wstring_view value)
      : value_(SysAllocStringLen(value.data(), static_cast<UINT>(value.size()))) {}
  ~ScopedBstr() { SysFreeString(value_); }
  ScopedBstr(const ScopedBstr&) = delete;
  ScopedBstr& operator=(const ScopedBstr&) = delete;
  [[nodiscard]] BSTR get() const { return value_; }
  [[nodiscard]] bool valid() const { return value_ != nullptr; }

 private:
  BSTR value_;
};

void clear_exception(EXCEPINFO& exception) {
  SysFreeString(exception.bstrSource);
  SysFreeString(exception.bstrDescription);
  SysFreeString(exception.bstrHelpFile);
}

HRESULT invoke(IDispatch* dispatch, DISPID id, VARIANTARG* arguments,
               UINT argument_count, VARIANT* result) {
  DISPPARAMS parameters{arguments, nullptr, argument_count, 0};
  EXCEPINFO exception{};
  UINT argument_error = 0;
  const HRESULT status = dispatch->Invoke(
      id, IID_NULL, LOCALE_USER_DEFAULT, DISPATCH_METHOD, &parameters, result,
      &exception, &argument_error);
  if (FAILED(status)) {
    std::fwprintf(stderr, L"IDispatch::Invoke(0x%lx) failed: 0x%08lx",
                  static_cast<unsigned long>(id),
                  static_cast<unsigned long>(status));
    if (exception.bstrDescription != nullptr) {
      std::fwprintf(stderr, L" (%ls)", exception.bstrDescription);
    }
    std::fputwc(L'\n', stderr);
  }
  clear_exception(exception);
  return status;
}

HRESULT invoke_long(IDispatch* dispatch, DISPID id, VARIANTARG* arguments,
                    UINT argument_count, LONG& value) {
  ScopedVariant result;
  const HRESULT status =
      invoke(dispatch, id, arguments, argument_count, result.get());
  if (FAILED(status)) {
    return status;
  }
  VARIANT converted;
  VariantInit(&converted);
  const HRESULT conversion = VariantChangeType(&converted, result.get(), 0, VT_I4);
  if (SUCCEEDED(conversion)) {
    value = V_I4(&converted);
  }
  VariantClear(&converted);
  return conversion;
}

VARIANTARG bstr_argument(BSTR value) {
  VARIANTARG argument;
  VariantInit(&argument);
  V_VT(&argument) = VT_BSTR;
  V_BSTR(&argument) = value;
  return argument;
}

VARIANTARG long_argument(LONG value) {
  VARIANTARG argument;
  VariantInit(&argument);
  V_VT(&argument) = VT_I4;
  V_I4(&argument) = value;
  return argument;
}

VARIANTARG long_reference(LONG* value) {
  VARIANTARG argument;
  VariantInit(&argument);
  V_VT(&argument) = VT_I4 | VT_BYREF;
  V_I4REF(&argument) = value;
  return argument;
}

VARIANTARG bstr_reference(BSTR* value) {
  VARIANTARG argument;
  VariantInit(&argument);
  V_VT(&argument) = VT_BSTR | VT_BYREF;
  V_BSTRREF(&argument) = value;
  return argument;
}

VARIANTARG variant_reference(VARIANT* value) {
  VARIANTARG argument;
  VariantInit(&argument);
  V_VT(&argument) = VT_VARIANT | VT_BYREF;
  V_VARIANTREF(&argument) = value;
  return argument;
}

class JvLink {
 public:
  JvLink() = default;

  static HRESULT create(JvLink& target) {
    return CoCreateInstance(kJvLinkClsid, nullptr, CLSCTX_ALL, IID_IDispatch,
                            reinterpret_cast<void**>(&target.dispatch_));
  }

  ~JvLink() {
    if (initialized_) {
      LONG ignored = 0;
      invoke_long(dispatch_, kClose, nullptr, 0, ignored);
    }
    if (dispatch_ != nullptr) {
      dispatch_->Release();
    }
  }
  JvLink(const JvLink&) = delete;
  JvLink& operator=(const JvLink&) = delete;

  HRESULT call_string(DISPID id, std::wstring_view value, LONG& result) {
    ScopedBstr string(value);
    if (!string.valid()) {
      return E_OUTOFMEMORY;
    }
    VARIANTARG argument = bstr_argument(string.get());
    return invoke_long(dispatch_, id, &argument, 1, result);
  }

  HRESULT call_long(DISPID id, LONG value, LONG& result) {
    VARIANTARG argument = long_argument(value);
    return invoke_long(dispatch_, id, &argument, 1, result);
  }

  HRESULT call_no_args(DISPID id, LONG& result) {
    return invoke_long(dispatch_, id, nullptr, 0, result);
  }

  HRESULT call_void(DISPID id) {
    ScopedVariant result;
    return invoke(dispatch_, id, nullptr, 0, result.get());
  }

  void mark_initialized() { initialized_ = true; }

  HRESULT open(const Config& config, LONG& read_count, LONG& download_count,
               BSTR& last_timestamp, LONG& result) {
    ScopedBstr data_spec(config.data_spec);
    ScopedBstr from_time(config.from_time);
    if (!data_spec.valid() || !from_time.valid()) {
      return E_OUTOFMEMORY;
    }
    VARIANTARG arguments[] = {
        bstr_reference(&last_timestamp), long_reference(&download_count),
        long_reference(&read_count),    long_argument(1),
        bstr_argument(from_time.get()), bstr_argument(data_spec.get()),
    };
    return invoke_long(dispatch_, kOpen, arguments, 6, result);
  }

  HRESULT gets(ScopedVariant& buffer, BSTR& filename, LONG& result) {
    VARIANTARG arguments[] = {bstr_reference(&filename), long_argument(kBufferSize),
                              variant_reference(buffer.get())};
    return invoke_long(dispatch_, kGets, arguments, 3, result);
  }

 private:
  IDispatch* dispatch_ = nullptr;
  bool initialized_ = false;
};

bool is_ascii_alphanumeric(wchar_t value) {
  return (value >= L'0' && value <= L'9') ||
         (value >= L'A' && value <= L'Z') ||
         (value >= L'a' && value <= L'z');
}

bool parse_positive_long(const wchar_t* text, LONG& value) {
  wchar_t* end = nullptr;
  const long parsed = std::wcstol(text, &end, 10);
  if (text == end || *end != L'\0' || parsed <= 0) {
    return false;
  }
  value = parsed;
  return true;
}

bool parse_config(int argc, wchar_t** argv, Config& config) {
  const wchar_t* raw_key = _wgetenv(L"JRA_VAN_DATALAB_KEY");
  if (raw_key == nullptr) {
    std::fwprintf(stderr, L"JRA_VAN_DATALAB_KEY is required.\n");
    return false;
  }
  for (const wchar_t* current = raw_key; *current != L'\0'; ++current) {
    if (*current != L'-') {
      config.service_key.push_back(*current);
    }
  }
  if (config.service_key.size() != 17 ||
      !std::all_of(config.service_key.begin(), config.service_key.end(),
                   is_ascii_alphanumeric)) {
    std::fwprintf(stderr, L"JRA_VAN_DATALAB_KEY must contain 17 alphanumeric characters.\n");
    return false;
  }

  for (int index = 1; index < argc; index += 2) {
    if (index + 1 >= argc) {
      std::fwprintf(stderr, L"Missing value for %ls.\n", argv[index]);
      return false;
    }
    const std::wstring_view option(argv[index]);
    const wchar_t* value = argv[index + 1];
    if (option == L"--data-spec") {
      config.data_spec = value;
    } else if (option == L"--from-time") {
      config.from_time = value;
    } else if (option == L"--output") {
      config.output = value;
    } else if (option == L"--save-path") {
      config.save_path = value;
    } else if (option == L"--limit") {
      if (!parse_positive_long(value, config.limit)) {
        std::fwprintf(stderr, L"--limit must be positive.\n");
        return false;
      }
    } else if (option == L"--timeout") {
      LONG seconds = 0;
      if (!parse_positive_long(value, seconds)) {
        std::fwprintf(stderr, L"--timeout must be positive.\n");
        return false;
      }
      config.timeout_ms = static_cast<ULONGLONG>(seconds) * 1000;
    } else {
      std::fwprintf(stderr, L"Unknown option: %ls\n", argv[index]);
      return false;
    }
  }
  if (config.from_time.empty() || config.output.empty()) {
    std::fwprintf(stderr, L"--from-time and --output are required.\n");
    return false;
  }
  return true;
}

bool require_jv_success(const wchar_t* operation, LONG result) {
  if (result >= 0) {
    return true;
  }
  std::fwprintf(stderr, L"%ls failed with %ld.\n", operation, result);
  return false;
}

bool write_all(HANDLE file, const void* data, DWORD size) {
  const auto* bytes = static_cast<const std::uint8_t*>(data);
  DWORD offset = 0;
  while (offset < size) {
    DWORD written = 0;
    if (!WriteFile(file, bytes + offset, size - offset, &written, nullptr) ||
        written == 0) {
      return false;
    }
    offset += written;
  }
  return true;
}

bool write_cp932_as_utf8(HANDLE file, const char* data, LONG size) {
  const int wide_size = MultiByteToWideChar(kCp932, 0, data, size, nullptr, 0);
  if (wide_size <= 0) {
    return false;
  }
  std::vector<wchar_t> wide(static_cast<std::size_t>(wide_size));
  if (MultiByteToWideChar(kCp932, 0, data, size, wide.data(), wide_size) !=
      wide_size) {
    return false;
  }
  const int utf8_size = WideCharToMultiByte(CP_UTF8, 0, wide.data(), wide_size,
                                            nullptr, 0, nullptr, nullptr);
  if (utf8_size <= 0) {
    return false;
  }
  std::vector<char> utf8(static_cast<std::size_t>(utf8_size));
  if (WideCharToMultiByte(CP_UTF8, 0, wide.data(), wide_size, utf8.data(),
                          utf8_size, nullptr, nullptr) != utf8_size) {
    return false;
  }
  return write_all(file, utf8.data(), static_cast<DWORD>(utf8.size()));
}

bool read_records(JvLink& link, const Config& config, LONG& written) {
  HANDLE output = CreateFileW(config.output.c_str(), GENERIC_WRITE, 0, nullptr,
                              CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);
  if (output == INVALID_HANDLE_VALUE) {
    std::fwprintf(stderr, L"Could not open output file: %ls\n", config.output.c_str());
    return false;
  }

  written = 0;
  bool success = true;
  while (written < config.limit) {
    ScopedVariant buffer;
    BSTR filename = nullptr;
    LONG result = 0;
    const HRESULT status = link.gets(buffer, filename, result);
    SysFreeString(filename);
    if (FAILED(status)) {
      success = false;
      break;
    }
    if (result == 0) {
      break;
    }
    if (result == -1) {
      continue;
    }
    if (!require_jv_success(L"JVGets", result)) {
      success = false;
      break;
    }
    SAFEARRAY* array = V_ARRAY(buffer.get());
    if (array == nullptr) {
      std::fwprintf(stderr, L"JVGets returned an invalid buffer.\n");
      success = false;
      break;
    }
    void* data = nullptr;
    if (FAILED(SafeArrayAccessData(array, &data))) {
      V_ARRAY(buffer.get()) = nullptr;
      SafeArrayDestroy(array);
      success = false;
      break;
    }
    const bool write_success =
        write_cp932_as_utf8(output, static_cast<const char*>(data), result);
    SafeArrayUnaccessData(array);
    V_VT(buffer.get()) = VT_EMPTY;
    V_ARRAY(buffer.get()) = nullptr;
    SafeArrayDestroy(array);
    if (!write_success) {
      std::fwprintf(stderr, L"Could not convert or write a JV-Data record.\n");
      success = false;
      break;
    }
    ++written;
  }
  CloseHandle(output);
  return success;
}

int run(const Config& config) {
  ComApartment apartment;
  if (FAILED(apartment.result())) {
    std::fwprintf(stderr, L"CoInitializeEx failed: 0x%08lx\n",
                  static_cast<unsigned long>(apartment.result()));
    return 1;
  }

  JvLink link;
  HRESULT status = JvLink::create(link);
  if (FAILED(status)) {
    std::fwprintf(stderr, L"CoCreateInstance failed: 0x%08lx\n",
                  static_cast<unsigned long>(status));
    return 1;
  }

  LONG result = 0;
  status = link.call_string(kInit, L"UNKNOWN", result);
  if (FAILED(status) || !require_jv_success(L"JVInit", result)) {
    return 1;
  }
  link.mark_initialized();

  status = link.call_string(kSetServiceKey, config.service_key, result);
  if (FAILED(status) ||
      (result != kAlreadyRegistered && !require_jv_success(L"JVSetServiceKey", result))) {
    return 1;
  }
  CreateDirectoryW(config.save_path.c_str(), nullptr);
  status = link.call_string(kSetSavePath, config.save_path, result);
  if (FAILED(status) || !require_jv_success(L"JVSetSavePath", result)) {
    return 1;
  }
  status = link.call_long(kSetSaveFlag, 1, result);
  if (FAILED(status) || !require_jv_success(L"JVSetSaveFlag", result)) {
    return 1;
  }

  LONG read_count = 0;
  LONG download_count = 0;
  BSTR last_timestamp = nullptr;
  status = link.open(config, read_count, download_count, last_timestamp, result);
  if (FAILED(status) || !require_jv_success(L"JVOpen", result)) {
    SysFreeString(last_timestamp);
    return 1;
  }

  const ULONGLONG deadline = GetTickCount64() + config.timeout_ms;
  while (download_count > 0) {
    status = link.call_no_args(kStatus, result);
    if (FAILED(status) || !require_jv_success(L"JVStatus", result)) {
      SysFreeString(last_timestamp);
      return 1;
    }
    if (result >= download_count) {
      break;
    }
    if (GetTickCount64() >= deadline) {
      link.call_void(kCancel);
      std::fwprintf(stderr, L"JV-Link download timed out.\n");
      SysFreeString(last_timestamp);
      return 1;
    }
    Sleep(250);
  }

  LONG written = 0;
  if (!read_records(link, config, written)) {
    SysFreeString(last_timestamp);
    return 1;
  }
  std::wprintf(L"JV-Link OK: files=%ld, downloads=%ld, records=%ld, last=%ls\n",
               read_count, download_count, written,
               last_timestamp == nullptr ? L"-" : last_timestamp);
  std::wprintf(L"Output: %ls\n", config.output.c_str());
  SysFreeString(last_timestamp);
  return 0;
}

}  // namespace

int wmain(int argc, wchar_t** argv) {
  Config config;
  if (!parse_config(argc, argv, config)) {
    return 2;
  }
  return run(config);
}
