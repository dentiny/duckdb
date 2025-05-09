#include "duckdb/common/assert.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types/bit.hpp"
#include "duckdb/common/types/string_type.hpp"

namespace duckdb {

// **** helper functions ****
static char ComputePadding(idx_t len) {
	return UnsafeNumericCast<char>((8 - (len % 8)) % 8);
}

idx_t Bit::ComputeBitstringLen(idx_t len) {
	idx_t result = len / 8;
	if (len % 8 != 0) {
		result++;
	}
	// additional first byte to store info on zero padding
	result++;
	return result;
}

static inline idx_t GetBitPadding(const bitstring_t &bit_string) {
	auto data = const_data_ptr_cast(bit_string.GetData());
	D_ASSERT(idx_t(data[0]) < 8);
	return data[0];
}

static inline idx_t GetBitSize(const string_t &str) {
	string error_message;
	idx_t str_len;
	if (!Bit::TryGetBitStringSize(str, str_len, &error_message)) {
		throw ConversionException(error_message);
	}
	return str_len;
}

uint8_t Bit::GetFirstByte(const bitstring_t &str) {
	D_ASSERT(str.GetSize() > 1);

	auto data = const_data_ptr_cast(str.GetData());
	return data[1] & ((1 << (8 - data[0])) - 1);
}

void Bit::Finalize(bitstring_t &str) {
	// bit strings require all padding bits to be set to 1
	// this method sets all padding bits to 1
	auto padding = GetBitPadding(str);
	for (idx_t i = 0; i < idx_t(padding); i++) {
		Bit::SetBitInternal(str, i, 1);
	}
	str.Finalize();
	Bit::Verify(str);
}

void Bit::SetEmptyBitString(bitstring_t &target, string_t &input) {
	char *res_buf = target.GetDataWriteable();
	const char *buf = input.GetData();
	memset(res_buf, 0, input.GetSize());
	res_buf[0] = buf[0];
	Bit::Finalize(target);
}

void Bit::SetEmptyBitString(bitstring_t &target, idx_t len) {
	char *res_buf = target.GetDataWriteable();
	memset(res_buf, 0, target.GetSize());
	res_buf[0] = ComputePadding(len);
	Bit::Finalize(target);
}

// **** casting functions ****
void Bit::ToString(bitstring_t bits, char *output) {
	auto data = const_data_ptr_cast(bits.GetData());
	auto len = bits.GetSize();

	idx_t padding = GetBitPadding(bits);
	idx_t output_idx = 0;
	for (idx_t bit_idx = padding; bit_idx < 8; bit_idx++) {
		output[output_idx++] = data[1] & (1 << (7 - bit_idx)) ? '1' : '0';
	}
	for (idx_t byte_idx = 2; byte_idx < len; byte_idx++) {
		for (idx_t bit_idx = 0; bit_idx < 8; bit_idx++) {
			output[output_idx++] = data[byte_idx] & (1 << (7 - bit_idx)) ? '1' : '0';
		}
	}
}

string Bit::ToString(bitstring_t str) {
	auto len = BitLength(str);
	auto buffer = make_unsafe_uniq_array_uninitialized<char>(len);
	ToString(str, buffer.get());
	return string(buffer.get(), len);
}

bool Bit::TryGetBitStringSize(string_t str, idx_t &str_len, string *error_message) {
	auto data = const_data_ptr_cast(str.GetData());
	auto len = str.GetSize();
	str_len = 0;
	for (idx_t i = 0; i < len; i++) {
		if (data[i] == '0' || data[i] == '1') {
			str_len++;
		} else {
			string error = StringUtil::Format("Invalid character encountered in string -> bit conversion: '%s'",
			                                  string(const_char_ptr_cast(data) + i, 1));
			HandleCastError::AssignError(error, error_message);
			return false;
		}
	}
	if (str_len == 0) {
		string error = "Cannot cast empty string to BIT";
		HandleCastError::AssignError(error, error_message);
		return false;
	}
	str_len = ComputeBitstringLen(str_len);
	return true;
}

void Bit::ToBit(string_t str, bitstring_t &output_str) {
	auto data = const_data_ptr_cast(str.GetData());
	auto len = str.GetSize();
	auto output = output_str.GetDataWriteable();

	char byte = 0;
	idx_t padded_byte = len % 8;
	for (idx_t i = 0; i < padded_byte; i++) {
		byte <<= 1;
		if (data[i] == '1') {
			byte |= 1;
		}
	}
	if (padded_byte != 0) {
		*(output++) = UnsafeNumericCast<char>((8 - padded_byte)); // the first byte contains the number of padded zeroes
	}
	*(output++) = byte;

	for (idx_t byte_idx = padded_byte; byte_idx < len; byte_idx += 8) {
		byte = 0;
		for (idx_t bit_idx = 0; bit_idx < 8; bit_idx++) {
			byte <<= 1;
			if (data[byte_idx + bit_idx] == '1') {
				byte |= 1;
			}
		}
		*(output++) = byte;
	}
	Bit::Finalize(output_str);
}

string Bit::ToBit(string_t str) {
	auto bit_len = GetBitSize(str);
	auto buffer = make_unsafe_uniq_array_uninitialized<char>(bit_len);
	bitstring_t output_str(buffer.get(), UnsafeNumericCast<uint32_t>(bit_len));
	Bit::ToBit(str, output_str);
	return output_str.GetString();
}

void Bit::BlobToBit(string_t blob, bitstring_t &output_str) {
	auto data = const_data_ptr_cast(blob.GetData());
	auto output = output_str.GetDataWriteable();
	idx_t size = blob.GetSize();

	*output = 0; // No padding
	memcpy(output + 1, data, size);
}

string Bit::BlobToBit(string_t blob) {
	auto buffer = make_unsafe_uniq_array_uninitialized<char>(blob.GetSize() + 1);
	bitstring_t output_str(buffer.get(), UnsafeNumericCast<uint32_t>(blob.GetSize() + 1));
	Bit::BlobToBit(blob, output_str);
	return output_str.GetString();
}

void Bit::BitToBlob(bitstring_t bit, string_t &output_blob) {
	D_ASSERT(bit.GetSize() == output_blob.GetSize() + 1);

	auto data = const_data_ptr_cast(bit.GetData());
	auto output = output_blob.GetDataWriteable();
	idx_t size = output_blob.GetSize();

	output[0] = static_cast<char>(GetFirstByte(bit));
	if (size >= 2) {
		++output;
		// First byte in bitstring contains amount of padded bits,
		// second byte in bitstring is the padded byte,
		// therefore the rest of the data starts at data + 2 (third byte)
		memcpy(output, data + 2, size - 1);
	}
}

string Bit::BitToBlob(bitstring_t bit) {
	D_ASSERT(bit.GetSize() > 1);

	auto buffer = make_unsafe_uniq_array_uninitialized<char>(bit.GetSize() - 1);
	string_t output_str(buffer.get(), UnsafeNumericCast<uint32_t>(bit.GetSize() - 1));
	Bit::BitToBlob(bit, output_str);
	return output_str.GetString();
}

// **** scalar functions ****
void Bit::BitString(const string_t &input, idx_t bit_length, bitstring_t &result) {
	char *res_buf = result.GetDataWriteable();
	const char *buf = input.GetData();

	auto padding = ComputePadding(bit_length);
	res_buf[0] = padding;
	auto padding_len = UnsafeNumericCast<idx_t>(padding);
	for (idx_t i = 0; i < bit_length; i++) {
		if (i < bit_length - input.GetSize()) {
			Bit::SetBitInternal(result, i + padding_len, 0);
		} else {
			idx_t bit = buf[i - (bit_length - input.GetSize())] == '1' ? 1 : 0;
			Bit::SetBitInternal(result, i + padding_len, bit);
		}
	}
	Bit::Finalize(result);
}

void Bit::ExtendBitString(const bitstring_t &input, idx_t bit_length, bitstring_t &result) {
	uint8_t *res_buf = reinterpret_cast<uint8_t *>(result.GetDataWriteable());

	auto padding = ComputePadding(bit_length);
	res_buf[0] = static_cast<uint8_t>(padding);

	idx_t original_length = Bit::BitLength(input);
	D_ASSERT(bit_length >= original_length);
	idx_t shift = bit_length - original_length;
	for (idx_t i = 0; i < bit_length; i++) {
		if (i < shift) {
			Bit::SetBit(result, i, 0);
		} else {
			idx_t bit = Bit::GetBit(input, i - shift);
			Bit::SetBit(result, i, bit);
		}
	}
	Bit::Finalize(result);
}

idx_t Bit::BitLength(bitstring_t bits) {
	return ((bits.GetSize() - 1) * 8) - GetBitPadding(bits);
}

idx_t Bit::OctetLength(bitstring_t bits) {
	return bits.GetSize() - 1;
}

idx_t Bit::BitCount(bitstring_t bits) {
	idx_t count = 0;
	const char *buf = bits.GetData();
	for (idx_t byte_idx = 1; byte_idx < OctetLength(bits) + 1; byte_idx++) {
		for (idx_t bit_idx = 0; bit_idx < 8; bit_idx++) {
			count += (buf[byte_idx] & (1 << bit_idx)) ? 1 : 0;
		}
	}
	return count - GetBitPadding(bits);
}

idx_t Bit::BitPosition(bitstring_t substring, bitstring_t bits) {
	const char *buf = bits.GetData();
	auto len = bits.GetSize();
	auto substr_len = BitLength(substring);
	idx_t substr_idx = 0;

	for (idx_t bit_idx = GetBitPadding(bits); bit_idx < 8; bit_idx++) {
		idx_t bit = buf[1] & (1 << (7 - bit_idx)) ? 1 : 0;
		if (bit == GetBit(substring, substr_idx)) {
			substr_idx++;
			if (substr_idx == substr_len) {
				return (bit_idx - GetBitPadding(bits)) - substr_len + 2;
			}
		} else {
			substr_idx = 0;
		}
	}

	for (idx_t byte_idx = 2; byte_idx < len; byte_idx++) {
		for (idx_t bit_idx = 0; bit_idx < 8; bit_idx++) {
			idx_t bit = buf[byte_idx] & (1 << (7 - bit_idx)) ? 1 : 0;
			if (bit == GetBit(substring, substr_idx)) {
				substr_idx++;
				if (substr_idx == substr_len) {
					return (((byte_idx - 1) * 8) + bit_idx - GetBitPadding(bits)) - substr_len + 2;
				}
			} else {
				substr_idx = 0;
			}
		}
	}
	return 0;
}

idx_t Bit::GetBit(bitstring_t bit_string, idx_t n) {
	return Bit::GetBitInternal(bit_string, n + GetBitPadding(bit_string));
}

idx_t Bit::GetBitIndex(idx_t n) {
	return n / 8 + 1;
}

idx_t Bit::GetBitInternal(bitstring_t bit_string, idx_t n) {
	const char *buf = bit_string.GetData();
	auto idx = Bit::GetBitIndex(n);
	D_ASSERT(idx < bit_string.GetSize());
	auto byte = buf[idx] >> (7 - (n % 8));
	return (byte & 1 ? 1 : 0);
}

void Bit::SetBit(bitstring_t &bit_string, idx_t n, idx_t new_value) {
	SetBitInternal(bit_string, n + GetBitPadding(bit_string), new_value);
	Bit::Finalize(bit_string);
}

void Bit::SetBitInternal(bitstring_t &bit_string, idx_t n, idx_t new_value) {
	uint8_t *buf = reinterpret_cast<uint8_t *>(bit_string.GetDataWriteable());

	auto idx = Bit::GetBitIndex(n);
	D_ASSERT(idx < bit_string.GetSize());
	auto shift_byte = UnsafeNumericCast<uint8_t>(1 << (7 - (n % 8)));
	if (new_value == 0) {
		shift_byte = ~shift_byte;
		buf[idx] &= shift_byte;
	} else {
		buf[idx] |= shift_byte;
	}
}

// **** BITWISE operators ****
void Bit::RightShift(const bitstring_t &bit_string, idx_t shift, bitstring_t &result) {
	uint8_t *res_buf = reinterpret_cast<uint8_t *>(result.GetDataWriteable());
	const uint8_t *buf = reinterpret_cast<const uint8_t *>(bit_string.GetData());

	res_buf[0] = buf[0];
	auto padding = GetBitPadding(result);
	for (idx_t i = 0; i < Bit::BitLength(result); i++) {
		if (i < shift) {
			Bit::SetBitInternal(result, i + padding, 0);
		} else {
			idx_t bit = Bit::GetBit(bit_string, i - shift);
			Bit::SetBitInternal(result, i + padding, bit);
		}
	}
	Bit::Finalize(result);
}

void Bit::LeftShift(const bitstring_t &bit_string, idx_t shift, bitstring_t &result) {
	uint8_t *res_buf = reinterpret_cast<uint8_t *>(result.GetDataWriteable());
	const uint8_t *buf = reinterpret_cast<const uint8_t *>(bit_string.GetData());

	res_buf[0] = buf[0];
	auto padding = GetBitPadding(result);
	for (idx_t i = 0; i < Bit::BitLength(bit_string); i++) {
		if (i < (Bit::BitLength(bit_string) - shift)) {
			idx_t bit = Bit::GetBit(bit_string, shift + i);
			Bit::SetBitInternal(result, i + padding, bit);
		} else {
			Bit::SetBitInternal(result, i + padding, 0);
		}
	}
	Bit::Finalize(result);
}

void Bit::BitwiseAnd(const bitstring_t &rhs, const bitstring_t &lhs, bitstring_t &result) {
	if (Bit::BitLength(lhs) != Bit::BitLength(rhs)) {
		throw InvalidInputException("Cannot AND bit strings of different sizes");
	}

	uint8_t *buf = reinterpret_cast<uint8_t *>(result.GetDataWriteable());
	const uint8_t *r_buf = reinterpret_cast<const uint8_t *>(rhs.GetData());
	const uint8_t *l_buf = reinterpret_cast<const uint8_t *>(lhs.GetData());

	buf[0] = l_buf[0];
	for (idx_t i = 1; i < lhs.GetSize(); i++) {
		buf[i] = l_buf[i] & r_buf[i];
	}
	Bit::Finalize(result);
}

void Bit::BitwiseOr(const bitstring_t &rhs, const bitstring_t &lhs, bitstring_t &result) {
	if (Bit::BitLength(lhs) != Bit::BitLength(rhs)) {
		throw InvalidInputException("Cannot OR bit strings of different sizes");
	}

	uint8_t *buf = reinterpret_cast<uint8_t *>(result.GetDataWriteable());
	const uint8_t *r_buf = reinterpret_cast<const uint8_t *>(rhs.GetData());
	const uint8_t *l_buf = reinterpret_cast<const uint8_t *>(lhs.GetData());

	buf[0] = l_buf[0];
	for (idx_t i = 1; i < lhs.GetSize(); i++) {
		buf[i] = l_buf[i] | r_buf[i];
	}
	Bit::Finalize(result);
}

void Bit::BitwiseXor(const bitstring_t &rhs, const bitstring_t &lhs, bitstring_t &result) {
	if (Bit::BitLength(lhs) != Bit::BitLength(rhs)) {
		throw InvalidInputException("Cannot XOR bit strings of different sizes");
	}

	uint8_t *buf = reinterpret_cast<uint8_t *>(result.GetDataWriteable());
	const uint8_t *r_buf = reinterpret_cast<const uint8_t *>(rhs.GetData());
	const uint8_t *l_buf = reinterpret_cast<const uint8_t *>(lhs.GetData());

	buf[0] = l_buf[0];
	for (idx_t i = 1; i < lhs.GetSize(); i++) {
		buf[i] = l_buf[i] ^ r_buf[i];
	}
	Bit::Finalize(result);
}

void Bit::BitwiseNot(const bitstring_t &input, bitstring_t &result) {
	uint8_t *result_buf = reinterpret_cast<uint8_t *>(result.GetDataWriteable());
	const uint8_t *buf = reinterpret_cast<const uint8_t *>(input.GetData());

	result_buf[0] = buf[0];
	for (idx_t i = 1; i < input.GetSize(); i++) {
		result_buf[i] = ~buf[i];
	}
	Bit::Finalize(result);
}

void Bit::Verify(const bitstring_t &input) {
#ifdef DEBUG
	// bit strings require all padding bits to be set to 1
	auto padding = GetBitPadding(input);
	for (idx_t i = 0; i < padding; i++) {
		D_ASSERT(Bit::GetBitInternal(input, i));
	}
	// verify bit respects the "normal" string_t rules (i.e. null padding for inlined strings, prefix matches)
	input.VerifyCharacters();
#endif
}

} // namespace duckdb
