package cn.kane.redisCluster.hash;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public enum HashAlgorithmEnum implements IHashAlgorithmInterface {

	RS_HASH, JS_HASH,FNV_HASH, KETAMA_HASH;

	public Integer hash(Object obj) {
		if (null == obj) {
			return null;
		} else {
			switch (this) {
			case RS_HASH:
				return RSHash.hash(obj);
			case JS_HASH:
				return JSHash.hash(obj);
			case FNV_HASH:
				return FNVHash.hash(obj);
			case KETAMA_HASH:
				return ketamaHash.hash(obj);
			default:
				return null;
			}
		}
	}

	IHashAlgorithmInterface RSHash = new IHashAlgorithmInterface() {
		public Integer hash(Object obj) {
			String str = obj.toString();
			int b = 378551;
			int a = 63689;
			long hash = 0;

			for (int i = 0; i < str.length(); i++) {
				hash = hash * a + str.charAt(i);
				a = a * b;
			}

			return (int) hash;
		}
	};
	IHashAlgorithmInterface JSHash = new IHashAlgorithmInterface() {
		public Integer hash(Object obj) {
			if (null == obj) {
				return 0;
			} else {
				String str = obj.toString();
				long hash = 1315423911;
				for (int i = 0; i < str.length(); i++) {
					hash ^= ((hash << 5) + str.charAt(i) + (hash >> 2));
				}
				return (int) hash;
			}
		}
	};
	IHashAlgorithmInterface PJHash = new IHashAlgorithmInterface() {
		public Integer hash(Object obj) {
			if (null == obj) {
				return 0;
			} else {
				String str = obj.toString();
				long BitsInUnsignedInt = (long) (4 * 8);
				long ThreeQuarters = (long) ((BitsInUnsignedInt * 3) / 4);
				long OneEighth = (long) (BitsInUnsignedInt / 8);
				long HighBits = (long) (0xFFFFFFFF) << (BitsInUnsignedInt - OneEighth);
				long hash = 0;
				long test = 0;

				for (int i = 0; i < str.length(); i++) {
					hash = (hash << OneEighth) + str.charAt(i);

					if ((test = hash & HighBits) != 0) {
						hash = ((hash ^ (test >> ThreeQuarters)) & (~HighBits));
					}
				}
				return (int) hash;
			}
		}
	};
	IHashAlgorithmInterface ELFHash = new IHashAlgorithmInterface() {
		public Integer hash(Object obj) {
			if (null == obj) {
				return 0;
			} else {
				String str = obj.toString();
				long hash = 0;
				long x = 0;

				for (int i = 0; i < str.length(); i++) {
					hash = (hash << 4) + str.charAt(i);

					if ((x = hash & 0xF0000000L) != 0) {
						hash ^= (x >> 24);
					}
					hash &= ~x;
				}

				return (int) hash;
			}
		}
	};
	IHashAlgorithmInterface BKDRHash = new IHashAlgorithmInterface() {
		public Integer hash(Object obj) {
			if (null == obj) {
				return 0;
			} else {
				String str = obj.toString();
				long seed = 131; // 31 131 1313 13131 131313 etc..
				long hash = 0;

				for (int i = 0; i < str.length(); i++) {
					hash = (hash * seed) + str.charAt(i);
				}
				return (int) hash;
			}
		}
	};
	IHashAlgorithmInterface SDBMHash = new IHashAlgorithmInterface() {
		public Integer hash(Object obj) {
			if (null == obj) {
				return 0;
			} else {
				String str = obj.toString();
				long hash = 5381;

				for (int i = 0; i < str.length(); i++) {
					hash = ((hash << 5) + hash) + str.charAt(i);
				}

				return (int) hash;
			}
		}
	};
	IHashAlgorithmInterface DJBHash = new IHashAlgorithmInterface() {
		public Integer hash(Object obj) {
			if (null == obj) {
				return 0;
			} else {
				String str = obj.toString();
				long hash = 0;
				for (int i = 0; i < str.length(); i++) {
					hash = str.charAt(i) + (hash << 6) + (hash << 16) - hash;
				}

				return (int) hash;
			}
		}
	};
	IHashAlgorithmInterface DEKHash = new IHashAlgorithmInterface() {
		public Integer hash(Object obj) {
			if (null == obj) {
				return 0;
			} else {
				String str = obj.toString();
				long hash = str.length();

				for (int i = 0; i < str.length(); i++) {
					hash = ((hash << 5) ^ (hash >> 27)) ^ str.charAt(i);
				}

				return (int) hash;
			}
		}
	};
	IHashAlgorithmInterface BPHash = new IHashAlgorithmInterface() {
		public Integer hash(Object obj) {
			if (null == obj) {
				return 0;
			} else {
				String str = obj.toString();
				long hash = 0;

				for (int i = 0; i < str.length(); i++) {
					hash = hash << 7 ^ str.charAt(i);
				}

				return (int) hash;
			}
		}
	};
	IHashAlgorithmInterface FNVHash = new IHashAlgorithmInterface() {
		public Integer hash(Object obj) {
			if (null == obj) {
				return 0;
			} else {
				String str = obj.toString();
				long fnv_prime = 0x811C9DC5;
				long hash = 0;

				for (int i = 0; i < str.length(); i++) {
					hash *= fnv_prime;
					hash ^= str.charAt(i);
				}

				return (int) hash;
			}
		}
	};
	IHashAlgorithmInterface APHash = new IHashAlgorithmInterface() {
		public Integer hash(Object obj) {
			if (null == obj) {
				return 0;
			} else {
				String str = obj.toString();
				long hash = 0xAAAAAAAA;

				for (int i = 0; i < str.length(); i++) {
					if ((i & 1) == 0) {
						hash ^= ((hash << 7) ^ str.charAt(i) * (hash >> 3));
					} else {
						hash ^= (~((hash << 11) + str.charAt(i) ^ (hash >> 5)));
					}
				}
				return (int) hash;
			}
		}
	};
	/* ketama */
	private static MessageDigest md5;
	IHashAlgorithmInterface ketamaHash = new IHashAlgorithmInterface() {
		public Integer hash(Object obj) {
			if (null == obj) {
				return 0;
			} else {
				String str = obj.toString();
				long hash = 0xAAAAAAAA;
				if (md5 == null) {
					try {
						md5 = MessageDigest.getInstance("MD5");
					} catch (NoSuchAlgorithmException e) {
						throw new IllegalStateException(
								"no md5 algorythm found");
					}
				}
				md5.reset();
				md5.update(str.getBytes());
				byte[] bKey = md5.digest();
				long res = ((long) (bKey[3] & 0xFF) << 24)
						| ((long) (bKey[2] & 0xFF) << 16)
						| ((long) (bKey[1] & 0xFF) << 8)
						| (long) (bKey[0] & 0xFF);
				hash = res & 0xffffffffL;
				return (int) hash;
			}
		}
	};

}
